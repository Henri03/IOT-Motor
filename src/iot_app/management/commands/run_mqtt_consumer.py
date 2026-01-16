import json
import paho.mqtt.client as mqtt
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from iot_app.models import MotorInfo, LiveData, TwinData, MalfunctionLog 
from asgiref.sync import sync_to_async
import traceback
from datetime import datetime
import asyncio

class Command(BaseCommand):
    help = 'Startet einen MQTT-Consumer, um Sensordaten zu empfangen und in der Datenbank zu speichern.'

    last_anomaly_state = {}

    def add_arguments(self, parser):
        parser.add_argument('--broker', type=str, default=settings.DOCKER_MQTT_BROKER_HOST, help='MQTT Broker Host (intern für Docker)')
        parser.add_argument('--port', type=int, default=settings.MQTT_BROKER_PORT, help='MQTT Broker Port')
        parser.add_argument('--topic_live', type=str, default=settings.MQTT_TOPIC_LIVE, help='MQTT Topic für Live-Daten')
        parser.add_argument('--topic_twin', type=str, default=settings.MQTT_TOPIC_TWIN, help='MQTT Topic für Twin-Daten')
        parser.add_argument('--topic_malfunction_info', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_INFO, help='MQTT Topic für Info-Meldungen')
        parser.add_argument('--topic_malfunction_warning', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_WARNING, help='MQTT Topic für Warnmeldungen')
        parser.add_argument('--topic_malfunction_error', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_ERROR, help='MQTT Topic für Fehlermeldungen')
        parser.add_argument('--deviation_threshold', type=float, default=getattr(settings, 'MQTT_DEVIATION_THRESHOLD_PERCENT', 10.0),
                            help='Prozentuale Abweichung zwischen Live- und Twin-Daten, die eine Warnung auslöst.')

    def handle(self, *args, **options):
        broker_address = options['broker']
        port = options['port']
        topic_live = options['topic_live']
        topic_twin = options['topic_twin']
        topic_malfunction_info = options['topic_malfunction_info']
        topic_malfunction_warning = options['topic_malfunction_warning']
        topic_malfunction_error = options['topic_malfunction_error']
        self.deviation_threshold = options['deviation_threshold']

        metrics_to_compare = ['current', 'voltage', 'rpm', 'vibration', 'temp', 'torque']
        for metric_name in metrics_to_compare:
            self.last_anomaly_state[metric_name] = False 

        self.stdout.write(self.style.SUCCESS(f"Starte MQTT-Consumer für den Motor"))
        self.stdout.write(self.style.SUCCESS(f"Verbinde mit MQTT-Broker: {broker_address}:{port}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Live-Daten-Topic: {topic_live}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Twin-Daten-Topic: {topic_twin}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Malfunction-Info-Topic: {topic_malfunction_info}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Malfunction-Warning-Topic: {topic_malfunction_warning}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Malfunction-Error-Topic: {topic_malfunction_error}"))
        self.stdout.write(self.style.SUCCESS(f"Abweichungsschwellenwert für Warnungen: {self.deviation_threshold}%"))

        client = mqtt.Client()
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.user_data_set({
            'topic_live': topic_live,
            'topic_twin': topic_twin,
            'topic_malfunction_info': topic_malfunction_info,
            'topic_malfunction_warning': topic_malfunction_warning,
            'topic_malfunction_error': topic_malfunction_error,
            'command_instance': self 
        })

        try:
            client.connect(broker_address, port, 60)
            client.loop_forever() 
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"MQTT-Verbindungsfehler: {e}"))
            self.stderr.write(self.style.ERROR("Stellen Sie sicher, dass der MQTT-Broker (z.B. Mosquitto) läuft und erreichbar ist."))

    def _on_connect(self, client, userdata, flags, rc):
        command_instance = userdata['command_instance']
        if rc == 0:
            command_instance.stdout.write(command_instance.style.SUCCESS("Verbunden mit MQTT Broker!"))
            client.subscribe(userdata['topic_live'])
            client.subscribe(userdata['topic_twin'])
            client.subscribe(userdata['topic_malfunction_info'])
            client.subscribe(userdata['topic_malfunction_warning'])
            client.subscribe(userdata['topic_malfunction_error'])
        else:
            command_instance.stderr.write(command_instance.style.ERROR(f"Verbindung fehlgeschlagen, Rückgabecode {rc}"))

    def _on_message(self, client, userdata, msg):
        command_instance = userdata['command_instance']
        topic_live = userdata['topic_live']
        topic_twin = userdata['topic_twin']
        topic_malfunction_info = userdata['topic_malfunction_info']
        topic_malfunction_warning = userdata['topic_malfunction_warning']
        topic_malfunction_error = userdata['topic_malfunction_error']

        try:
            payload = json.loads(msg.payload.decode())
            command_instance.stdout.write(f"DEBUG: Nachricht auf Topic {msg.topic} empfangen: {payload}")

            motor = MotorInfo.objects.first()
            if not motor:
                command_instance.stderr.write(command_instance.style.ERROR("Kein Motor in der Datenbank gefunden. Bitte im Admin-Bereich erstellen."))

            if msg.topic == topic_live:
                self._save_live_data(payload)
                async_to_sync(self._notify_dashboard_group)("plot_data_point")
            elif msg.topic == topic_twin:
                self._save_twin_data(payload)
                async_to_sync(self._notify_dashboard_group)("plot_data_point")
            elif msg.topic == topic_malfunction_info or \
                 msg.topic == topic_malfunction_warning or \
                 msg.topic == topic_malfunction_error:
                self._save_malfunction_log(payload)
                
            async_to_sync(self._process_and_notify_dashboard)(
                command_instance.deviation_threshold,
                command_instance.last_anomaly_state,
            )

        except json.JSONDecodeError:
            command_instance.stderr.write(command_instance.style.ERROR(f"Fehler beim Dekodieren von JSON aus der Nachricht: {msg.payload}"))
        except Exception as e:
            command_instance.stderr.write(command_instance.style.ERROR(f"An error occurred while processing MQTT message: {e}"))
            traceback.print_exc()

    
    #  Hilfsfunktion zum Senden an den Channels Layer
    async def _notify_dashboard_group(self, message_type, data=None):
        channel_layer = get_channel_layer()
        if channel_layer:
            await channel_layer.group_send(
                "iot_dashboard_group", 
                {
                    "type": "dashboard_message", 
                    "message_type": message_type, 
                    "data": data,
                }
            )
        else:
            print("WARNING: Channel layer not available. Cannot notify dashboard.")

    async def _process_and_notify_dashboard(self, deviation_threshold, last_anomaly_state):
        """
        Ruft die neuesten Dashboard-Daten ab, aktualisiert den Anomalie-Status,
        erstellt entsprechende Logs bei Änderungen und sendet ein allgemeines
        Dashboard-Update an den Channel Layer.
        """
        channel_layer = get_channel_layer()
        
        await asyncio.sleep(0.1) 

        motor = await sync_to_async(MotorInfo.objects.first)()
        if not motor:
            print("DEBUG: [MQTT_Consumer] Kein Motor gefunden, kann kein Dashboard-Update senden.")
            retracted_count = "N/A"
            extended_count = "N/A"
        else:
            retracted_count = await sync_to_async(MalfunctionLog.objects.filter(
                description__icontains='motor fährt ein', message_type='INFO'
            ).count)()
            extended_count = await sync_to_async(MalfunctionLog.objects.filter(
                description__icontains='motor fährt aus', message_type='INFO'
            ).count)()


        latest_live_data = await sync_to_async(LiveData.objects.order_by('-timestamp').first)()
        latest_twin_data = await sync_to_async(TwinData.objects.order_by('-timestamp').first)()
        
        latest_malfunction_logs_for_display = await sync_to_async(list)(MalfunctionLog.objects.order_by('-timestamp')[:5].values())

        real_motor_data = {
            "Strom": {"value": latest_live_data.current if latest_live_data else None, "unit": "A"},
            "Spannung": {"value": latest_live_data.voltage if latest_live_data else None, "unit": "V"},
            "Drehzahl": {"value": latest_live_data.rpm if latest_live_data else None, "unit": "U/min"},
            "Vibration": {"value": latest_live_data.vibration if latest_live_data else None, "unit": "mm/s"},
            "Temperatur": {"value": latest_live_data.temp if latest_live_data else None, "unit": "°C"},
            "Drehmoment": {"value": latest_live_data.torque if latest_live_data else None, "unit": "Nm"},
            "Laufzeit": {"value": latest_live_data.run_time if latest_live_data else None, "unit": "h"},
            
            "Anzahl eingefahren": {"value": retracted_count, "unit": ""},
            "Anzahl ausgefahren": {"value": extended_count, "unit": ""},
        }

        digital_twin_data = {
            "Strom": {"value": latest_twin_data.current if latest_twin_data else None, "unit": "A"},
            "Spannung": {"value": latest_twin_data.voltage if latest_twin_data else None, "unit": "V"},
            "Drehzahl": {"value": latest_twin_data.rpm if latest_twin_data else None, "unit": "U/min"},
            "Vibration": {"value": latest_twin_data.vibration if latest_twin_data else None, "unit": "mm/s"},
            "Temperatur": {"value": latest_twin_data.temp if latest_twin_data else None, "unit": "°C"},
            "Drehmoment": {"value": latest_twin_data.torque if latest_twin_data else None, "unit": "Nm"},
            "Laufzeit": {"value": latest_twin_data.run_time if latest_twin_data else None, "unit": "h"},
            "Anzahl eingefahren": {"value": retracted_count, "unit": ""},
            "Anzahl ausgefahren": {"value": extended_count, "unit": ""},
        }

        current_anomaly_detected = False
        current_anomaly_message = "Motor läuft normal."
        
        metrics_to_compare = ['current', 'voltage', 'rpm', 'vibration', 'temp', 'torque']
        
        logs_to_create = []

        if not latest_live_data or not latest_twin_data:
            current_anomaly_message = "Keine ausreichenden Daten für Anomalie-Erkennung verfügbar."
            current_anomaly_detected = True
        else:
            for metric_name in metrics_to_compare:
                live_value = getattr(latest_live_data, metric_name, None)
                twin_value = getattr(latest_twin_data, metric_name, None)
                
                is_currently_deviating = False
                if live_value is not None and twin_value is not None:
                    if twin_value != 0:
                        deviation = abs((live_value - twin_value) / twin_value) * 100
                        if deviation > deviation_threshold:
                            is_currently_deviating = True
                            current_anomaly_detected = True
                            if "KRITISCHE STÖRUNG" not in current_anomaly_message:
                                current_anomaly_message = f"WARNUNG: {metric_name.capitalize()}-Abweichung erkannt."
                    elif live_value != 0 and twin_value == 0:
                        is_currently_deviating = True
                        current_anomaly_detected = True
                        if "KRITISCHE STÖRUNG" not in current_anomaly_message:
                            current_anomaly_message = f"WARNUNG: {metric_name.capitalize()}-Abweichung: Twin ist 0, Live ist {live_value:.2f}."
                
                if is_currently_deviating and not last_anomaly_state.get(metric_name, False):
                    logs_to_create.append({
                        'message_type': 'WARNING',
                        'description': f"{metric_name.capitalize()}-Abweichung > {deviation_threshold:.1f}% erkannt (Live: {live_value:.2f}, Twin: {twin_value:.2f})",
                        'motor_state': 'unbekannt',
                        'emergency_stop_active': False
                    })
                    last_anomaly_state[metric_name] = True
                elif not is_currently_deviating and last_anomaly_state.get(metric_name, False):
                    logs_to_create.append({
                        'message_type': 'INFO',
                        'description': f"{metric_name.capitalize()}-Abweichung hat sich behoben. Motor läuft wieder normal.",
                        'motor_state': 'normal',
                        'emergency_stop_active': False
                    })
                    last_anomaly_state[metric_name] = False
            
            for log_data in logs_to_create:
                await sync_to_async(MalfunctionLog.objects.create)(**log_data)

            recent_logs = await sync_to_async(list)(MalfunctionLog.objects.filter(
                timestamp__gte=timezone.now() - timezone.timedelta(minutes=5)
            ).order_by('-timestamp').values())
            
            error_logs = [log for log in recent_logs if log['message_type'] == 'ERROR']
            if error_logs:
                current_anomaly_detected = True
                current_anomaly_message = f"KRITISCHE STÖRUNG: {error_logs[0]['description']}"

        message_to_send = {
            'real_motor_data': real_motor_data,
            'digital_twin_data': digital_twin_data,
            'anomaly_status': {
                'detected': current_anomaly_detected,
                'message': current_anomaly_message
            },
            'malfunction_logs': latest_malfunction_logs_for_display
        }

        serializable_message_data = _to_serializable_dict(message_to_send)

        try:
            await channel_layer.group_send(
                "iot_dashboard_group", 
                {
                    'type': 'dashboard_message',
                    'message_type': 'dashboard_update', 
                    'data': serializable_message_data 
                }
            )

        except Exception as e:
            print(f"DEBUG: [MQTT_Consumer] Fehler beim Senden des Dashboard-Updates an Channel Layer: {e}")

    def _save_live_data(self, payload):
        """Speichert Live-Daten in der Datenbank."""
        LiveData.objects.create(
            timestamp=timezone.now(),
            current=payload.get('current'),
            voltage=payload.get('voltage'),
            rpm=payload.get('rpm'),
            vibration=payload.get('vibration'),
            temp=payload.get('temp'),
            torque=payload.get('torque'),
            run_time=payload.get('run_time'),
        )

    def _save_twin_data(self, payload):
        """Speichert Twin-Daten in der Datenbank."""
        TwinData.objects.create(
            timestamp=timezone.now(),
            current=payload.get('current'),
            voltage=payload.get('voltage'),
            rpm=payload.get('rpm'),
            vibration=payload.get('vibration'),
            temp=payload.get('temp'),
            torque=payload.get('torque'),
            run_time=payload.get('run_time'),
        )

    def _save_malfunction_log(self, payload):
        """Speichert eine Störmeldung (Info, Warning, Error) in der Datenbank."""
        MalfunctionLog.objects.create(
            timestamp=timezone.now(),
            message_type=payload.get('message_type'),
            description=payload.get('description'),
            motor_state=payload.get('motor_state', 'unbekannt'),
            emergency_stop_active=payload.get('emergency_stop_active', False),
        )

# Globale Hilfsfunktion für die Serialisierung
def _to_serializable_dict(obj):
    """
    Rekursive Hilfsfunktion, die datetime-Objekte in einem Dictionary/einer Liste
    in ISO 8601 Strings umwandelt, damit sie über Channels gesendet werden können.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_serializable_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_serializable_dict(elem) for elem in obj]
    return obj