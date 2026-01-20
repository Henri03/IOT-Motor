# path: IOT_PROJECT/src/iot_app/management/commands/mqtt_consumer.py
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

class Command(BaseCommand):             # erlaubt es, das Skript mit 'python manage.py mqtt_consumer' in der Datei "docker-compose.yml" auszuführen.
    help = 'Startet einen MQTT-Consumer, um Sensordaten zu empfangen und in der Datenbank zu speichern.'

    last_anomaly_state = {}             # Speichert den letzten Anomalie-Status pro Metrik

    def add_arguments(self, parser):
        """
        Definiert die Kommandozeilenargumente für den MQTT-Consumer.
        """
        parser.add_argument('--broker', type=str, default=settings.DOCKER_MQTT_BROKER_HOST, help='MQTT Broker Host (intern für Docker)')
        parser.add_argument('--port', type=int, default=settings.MQTT_BROKER_PORT, help='MQTT Broker Port')
        parser.add_argument('--topic_live', type=str, default=settings.MQTT_TOPIC_LIVE, help='MQTT Topic für Live-Daten')
        parser.add_argument('--topic_twin', type=str, default=settings.MQTT_TOPIC_TWIN, help='MQTT Topic für Twin-Daten')
        parser.add_argument('--topic_malfunction_info', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_INFO, help='MQTT Topic für Info-Meldungen')
        parser.add_argument('--topic_malfunction_warning', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_WARNING, help='MQTT Topic für Warnmeldungen')
        parser.add_argument('--topic_malfunction_error', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_ERROR, help='MQTT Topic für Fehlermeldungen')
        parser.add_argument('--deviation_threshold', type=float, default=getattr(settings, 'MQTT_DEVIATION_THRESHOLD_PERCENT', 10.0),
                            help='Prozentuale Abweichung zwischen Live- und Twin-Daten, die eine Warnung auslöst.')

        parser.add_argument('--topic_raw_temperature', type=str, default=getattr(settings, 'MQTT_TOPIC_RAW_TEMPERATURE', "raw/temperature"), help='MQTT Topic für Rohdaten Temperatur')
        parser.add_argument('--topic_raw_current', type=str, default=getattr(settings, 'MQTT_TOPIC_RAW_CURRENT', "raw/current"), help='MQTT Topic für Rohdaten Strom')
        parser.add_argument('--topic_raw_torque', type=str, default=getattr(settings, 'MQTT_TOPIC_RAW_TORQUE', "raw/torque"), help='MQTT Topic für Rohdaten Drehmoment')

        parser.add_argument('--topic_feature_temperature', type=str, default=getattr(settings, 'MQTT_TOPIC_FEATURE_TEMPERATURE', "feature/temperature"), help='MQTT Topic für Feature Temperatur')
        parser.add_argument('--topic_feature_current', type=str, default=getattr(settings, 'MQTT_TOPIC_FEATURE_CURRENT', "feature/current"), help='MQTT Topic für Feature Strom')
        parser.add_argument('--topic_feature_torque', type=str, default=getattr(settings, 'MQTT_TOPIC_FEATURE_TORQUE', "feature/torque"), help='MQTT Topic für Feature Drehmoment')

        parser.add_argument('--topic_prediction_temperature', type=str, default=getattr(settings, 'MQTT_TOPIC_PREDICTION_TEMPERATURE', "prediction/temperature"), help='MQTT Topic für Vorhersage Temperatur')
        parser.add_argument('--topic_prediction_current', type=str, default=getattr(settings, 'MQTT_TOPIC_PREDICTION_CURRENT', "prediction/current"), help='MQTT Topic für Vorhersage Strom')
        parser.add_argument('--topic_prediction_torque', type=str, default=getattr(settings, 'MQTT_TOPIC_PREDICTION_TORQUE', "prediction/torque"), help='MQTT Topic für Vorhersage Drehmoment')

    def handle(self, *args, **options):
        """
        Die Hauptmethode, die beim Ausführen des Management-Befehls aufgerufen wird.
        Initialisiert den MQTT-Client und startet die Verbindung.
        """
        self.broker_address = options['broker']    # options['broker'] enthält den Wert aus settings.DOCKER_MQTT_BROKER_HOST (oder Kommandozeile)
        self.port = options['port']
        self.topic_live = options['topic_live']    # self.topic_live erhält den Wert aus settings.MQTT_TOPIC_LIVE (oder Kommandozeile)
        self.topic_twin = options['topic_twin']
        self.topic_malfunction_info = options['topic_malfunction_info']
        self.topic_malfunction_warning = options['topic_malfunction_warning']
        self.topic_malfunction_error = options['topic_malfunction_error']
        self.deviation_threshold = options['deviation_threshold']

        self.topic_raw_temperature = options['topic_raw_temperature']
        self.topic_raw_current = options['topic_raw_current']
        self.topic_raw_torque = options['topic_raw_torque']

        self.topic_feature_temperature = options['topic_feature_temperature']
        self.topic_feature_current = options['topic_feature_current']
        self.topic_feature_torque = options['topic_feature_torque']

        self.topic_prediction_temperature = options['topic_prediction_temperature']
        self.topic_prediction_current = options['topic_prediction_current']
        self.topic_prediction_torque = options['topic_prediction_torque']

        # Initialisiert den Anomalie-Status für jede Metrik
        metrics_to_compare = ['current', 'voltage', 'rpm', 'vibration', 'temp', 'torque']
        for metric_name in metrics_to_compare:
            self.last_anomaly_state[metric_name] = False

        self.stdout.write(self.style.SUCCESS(f"Starte MQTT-Consumer für den Motor"))
        self.stdout.write(self.style.SUCCESS(f"Verbinde mit MQTT-Broker: {self.broker_address}:{self.port}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Live-Daten-Topic: {self.topic_live}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Twin-Daten-Topic: {self.topic_twin}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Malfunction-Info-Topic: {self.topic_malfunction_info}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Malfunction-Warning-Topic: {self.topic_malfunction_warning}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Malfunction-Error-Topic: {self.topic_malfunction_error}"))

        self.stdout.write(self.style.SUCCESS(f"Abonniere Rohdaten-Topics: {self.topic_raw_temperature}, {self.topic_raw_current}, {self.topic_raw_torque}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Feature-Topics: {self.topic_feature_temperature}, {self.topic_feature_current}, {self.topic_feature_torque}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Vorhersage-Topics: {self.topic_prediction_temperature}, {self.topic_prediction_current}, {self.topic_prediction_torque}"))

        self.stdout.write(self.style.SUCCESS(f"Abweichungsschwellenwert für Warnungen: {self.deviation_threshold}%"))

        client = mqtt.Client()
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.user_data_set({
            'command_instance': self # speichert das aktuelle Command-Objekt (self) unter dem Schlüssel 'command_instance' in das userdata-Dictionary des MQTT-Clients
        })

        try:
            client.connect(self.broker_address, self.port, 60)
            client.loop_forever() # Startet die MQTT-Client-Schleife, blockiert den Thread
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"MQTT-Verbindungsfehler: {e}"))
            self.stderr.write(self.style.ERROR("Stellen Sie sicher, dass der MQTT-Broker (Mosquitto) läuft und erreichbar ist."))

    def _on_connect(self, client, userdata, flags, rc):
        """
        Callback-Funktion, die aufgerufen wird, wenn der Client eine Verbindung zum Broker herstellt.
        Abonniert die konfigurierten MQTT-Topics.
        """
        command_instance = userdata['command_instance']     ########################################################
        if rc == 0: #return code == 0 -> keine fehler
            command_instance.stdout.write(command_instance.style.SUCCESS("Verbunden mit MQTT Broker!"))
            client.subscribe(command_instance.topic_live)
            client.subscribe(command_instance.topic_twin)
            client.subscribe(command_instance.topic_malfunction_info)
            client.subscribe(command_instance.topic_malfunction_warning)
            client.subscribe(command_instance.topic_malfunction_error)

            client.subscribe(command_instance.topic_raw_temperature)
            client.subscribe(command_instance.topic_raw_current)
            client.subscribe(command_instance.topic_raw_torque)
            client.subscribe(command_instance.topic_feature_temperature)
            client.subscribe(command_instance.topic_feature_current)
            client.subscribe(command_instance.topic_feature_torque)
            client.subscribe(command_instance.topic_prediction_temperature)
            client.subscribe(command_instance.topic_prediction_current)
            client.subscribe(command_instance.topic_prediction_torque)
        else:
            command_instance.stderr.write(command_instance.style.ERROR(f"Verbindung fehlgeschlagen, Rückgabecode {rc}"))

    def _on_message(self, client, userdata, msg):
        """
        Callback-Funktion, die aufgerufen wird, wenn eine Nachricht auf einem abonnierten Topic empfangen wird.
        Leitet die Nachricht an den entsprechenden Handler weiter.
        """
        command_instance = userdata['command_instance']
        
        try:
            payload = json.loads(msg.payload.decode())
            command_instance.stdout.write(f"DEBUG: Nachricht auf Topic {msg.topic} empfangen: {payload}")

            # Überprüfen, ob ein Motor in der Datenbank existiert
            # Dies ist wichtig für die Anomalie-Erkennung und Dashboard-Updates
            motor = MotorInfo.objects.first()
            if not motor:
                command_instance.stderr.write(command_instance.style.ERROR("Kein Motor in der Datenbank gefunden. Bitte im Admin-Bereich erstellen."))
                return

            # --- Topic-basierte Nachrichtenverarbeitung ---
            if msg.topic == command_instance.topic_live:
                command_instance._handle_live_data(payload)
            elif msg.topic == command_instance.topic_twin:
                command_instance._handle_twin_data(payload)
            elif msg.topic == command_instance.topic_malfunction_info:
                command_instance._handle_malfunction_data(payload, 'INFO')
            elif msg.topic == command_instance.topic_malfunction_warning:
                command_instance._handle_malfunction_data(payload, 'WARNING')
            elif msg.topic == command_instance.topic_malfunction_error:
                command_instance._handle_malfunction_data(payload, 'ERROR')
            elif msg.topic == command_instance.topic_raw_temperature:
                command_instance._handle_raw_data(payload, 'temperature')
            elif msg.topic == command_instance.topic_raw_current:
                command_instance._handle_raw_data(payload, 'current')
            elif msg.topic == command_instance.topic_raw_torque:
                command_instance._handle_raw_data(payload, 'torque')
            elif msg.topic == command_instance.topic_feature_temperature:
                command_instance._handle_feature_data(payload, 'temperature')
            elif msg.topic == command_instance.topic_feature_current:
                command_instance._handle_feature_data(payload, 'current')
            elif msg.topic == command_instance.topic_feature_torque:
                command_instance._handle_feature_data(payload, 'torque')
            elif msg.topic == command_instance.topic_prediction_temperature:
                command_instance._handle_prediction_data(payload, 'temperature')
            elif msg.topic == command_instance.topic_prediction_current:
                command_instance._handle_prediction_data(payload, 'current')
            elif msg.topic == command_instance.topic_prediction_torque:
                command_instance._handle_prediction_data(payload, 'torque')
            else:
                command_instance.stdout.write(f"INFO: Unbekanntes Topic empfangen: {msg.topic}")

            # Nach jeder Nachricht die Dashboard-Anzeige aktualisieren und Anomalien prüfen
            async_to_sync(command_instance._process_and_notify_dashboard)(
                command_instance.deviation_threshold,
                command_instance.last_anomaly_state,
            )

        except json.JSONDecodeError:
            command_instance.stderr.write(command_instance.style.ERROR(f"Fehler beim Dekodieren von JSON aus der Nachricht: {msg.payload} auf Topic {msg.topic}"))
        except Exception as e:
            command_instance.stderr.write(command_instance.style.ERROR(f"Ein unerwarteter Fehler bei der Verarbeitung der MQTT-Nachricht auf Topic {msg.topic}: {e}"))
            traceback.print_exc()

    def _handle_live_data(self, payload):
        """
        Verarbeitet Nachrichten vom Live-Daten-Topic.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self._save_live_data(payload)
        async_to_sync(self._notify_dashboard_group)("plot_data_point")
        self.stdout.write(self.style.SUCCESS(f"Live-Daten gespeichert und Dashboard benachrichtigt."))

    def _handle_twin_data(self, payload):
        """
        Verarbeitet Nachrichten vom Twin-Daten-Topic.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self._save_twin_data(payload)
        async_to_sync(self._notify_dashboard_group)("plot_data_point")
        self.stdout.write(self.style.SUCCESS(f"Twin-Daten gespeichert und Dashboard benachrichtigt."))

    def _handle_malfunction_data(self, payload, topic_type):
        """
        Verarbeitet Nachrichten von den Malfunction-Topics (Info, Warning, Error).
        Speichert die Daten im MalfunctionLog.
        """
        # Ergänze den message_type im Payload, falls nicht vorhanden
        if 'message_type' not in payload:
            payload['message_type'] = topic_type
        self._save_malfunction_log(payload)
        self.stdout.write(self.style.WARNING(f"Störungsprotokoll ({topic_type}) gespeichert: {payload.get('description')}"))

    def _handle_raw_data(self, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Rohdaten-Topics.
        Aktuell: Nur Debug-Ausgabe und Dashboard-Benachrichtigung.
        """
        self.stdout.write(self.style.SUCCESS(f"Rohdaten für {metric_type} empfangen: {payload}. Noch keine Speicherung."))
        # Benachrichtigt das Dashboard, dass neue Daten angekommen sind (auch wenn sie noch nicht gespeichert werden)
        

    def _handle_feature_data(self, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Feature-Topics.
        Aktuell: Nur Debug-Ausgabe und Dashboard-Benachrichtigung.
        """
        self.stdout.write(self.style.SUCCESS(f"Feature-Daten für {metric_type} empfangen: {payload}. Noch keine Speicherung."))
        # Benachrichtigt das Dashboard, dass neue Daten angekommen sind (auch wenn sie noch nicht gespeichert werden)
        

    def _handle_prediction_data(self, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Vorhersage-Topics.
        Aktuell: Nur Debug-Ausgabe und Dashboard-Benachrichtigung.
        """
        self.stdout.write(self.style.SUCCESS(f"Vorhersagedaten für {metric_type} empfangen: {payload}. Noch keine Speicherung."))
        # Benachrichtigt das Dashboard, dass neue Daten angekommen sind (auch wenn sie noch nicht gespeichert werden)
        




    # --- Hilfsfunktionen für Datenbankoperationen und Channels-Kommunikation ---

    async def _notify_dashboard_group(self, message_type, data=None):
        """
        Asynchrone Hilfsfunktion zum Senden einer Nachricht an die Dashboard-Gruppe
        über den Channels Layer.
        """
        channel_layer = get_channel_layer()
        if channel_layer:
            await channel_layer.group_send(
                "iot_dashboard_group",
                {
                    "type": "dashboard_message", # Muss mit einer Methode im Consumer übereinstimmen
                    "message_type": message_type, # Eigener Typ für die interne Logik des Consumers
                    "data": data,
                }
            )
        else:
            print("WARNING: Channel layer not available. Cannot notify dashboard.")

    async def _process_and_notify_dashboard(self, deviation_threshold, last_anomaly_state):
        """
        Ruft die neuesten Dashboard-relevanten Daten ab, führt Anomalie-Erkennung durch,
        erstellt bei Bedarf MalfunctionLogs und sendet ein umfassendes Update
        an alle verbundenen Dashboard-Clients über den Channels Layer.
        """
        channel_layer = get_channel_layer()
        if not channel_layer:
            print("DEBUG: [MQTT_Consumer] Channel layer nicht verfügbar, Dashboard-Update übersprungen.")
            return

        # Kleine Verzögerung, um sicherzustellen, dass Daten in der DB sind und um den Event-Loop nicht zu überlasten
        await asyncio.sleep(0.05)

        motor = await sync_to_async(MotorInfo.objects.first)()
        if not motor:
            print("DEBUG: [MQTT_Consumer] Kein Motor gefunden, kann kein detailliertes Dashboard-Update senden.")
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

        # Nur die neuesten 5 Logs für die Anzeige abrufen
        latest_malfunction_logs_for_display = await sync_to_async(list)(
            MalfunctionLog.objects.order_by('-timestamp')[:5].values()
        )

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

        logs_to_create = [] # Sammelt Logs, die in dieser Iteration erstellt werden sollen

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
                            if "KRITISCHE STÖRUNG" not in current_anomaly_message: # Überschreibe nicht kritische Meldungen
                                current_anomaly_message = f"WARNUNG: {metric_name.capitalize()}-Abweichung erkannt."
                    elif live_value != 0 and twin_value == 0: # Sonderfall: Twin ist 0, Live nicht
                        is_currently_deviating = True
                        current_anomaly_detected = True
                        if "KRITISCHE STÖRUNG" not in current_anomaly_message:
                            current_anomaly_message = f"WARNUNG: {metric_name.capitalize()}-Abweichung: Twin ist 0, Live ist {live_value:.2f}."

                # Logik zur Erstellung von MalfunctionLogs bei Statusänderungen
                if is_currently_deviating and not last_anomaly_state.get(metric_name, False):
                    logs_to_create.append({
                        'message_type': 'WARNING',
                        'description': f"{metric_name.capitalize()}-Abweichung > {deviation_threshold:.1f}% erkannt (Live: {live_value:.2f}, Twin: {twin_value:.2f})",
                        'motor_state': 'unbekannt', # Oder basierend auf weiteren Daten bestimmen
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

            # Speichere gesammelte Logs in der Datenbank
            for log_data in logs_to_create:
                await sync_to_async(MalfunctionLog.objects.create)(**log_data)

            # Prüfe auf aktuelle ERROR-Logs, die die Anomalie-Meldung überschreiben könnten
            recent_logs = await sync_to_async(list)(MalfunctionLog.objects.filter(
                timestamp__gte=timezone.now() - timezone.timedelta(minutes=5) # Nur Logs der letzten 5 Minuten prüfen
            ).order_by('-timestamp').values())

            error_logs = [log for log in recent_logs if log['message_type'] == 'ERROR']
            if error_logs:
                current_anomaly_detected = True
                current_anomaly_message = f"KRITISCHE STÖRUNG: {error_logs[0]['description']}" # Zeige den neuesten Fehler an

        message_to_send = {
            'real_motor_data': real_motor_data,
            'digital_twin_data': digital_twin_data,
            'anomaly_status': {
                'detected': current_anomaly_detected,
                'message': current_anomaly_message
            },
            'malfunction_logs': latest_malfunction_logs_for_display
        }

        # Konvertiere alle datetime-Objekte in ISO-Strings für die JSON-Serialisierung
        serializable_message_data = _to_serializable_dict(message_to_send)

        try:
            await channel_layer.group_send(
                "iot_dashboard_group",
                {
                    'type': 'dashboard_message', # Methode im Consumer, die diese Nachricht verarbeitet
                    'message_type': 'dashboard_update', # Interner Typ für den Consumer
                    'data': serializable_message_data # Die tatsächlichen Daten
                }
            )
        except Exception as e:
            print(f"DEBUG: [MQTT_Consumer] Fehler beim Senden des Dashboard-Updates an Channel Layer: {e}")
            traceback.print_exc() # Für detailliertere Fehlerinformationen

    @sync_to_async
    def _save_live_data(self, payload):
        """Speichert Live-Daten in der Datenbank (synchroner ORM-Aufruf)."""
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

    @sync_to_async
    def _save_twin_data(self, payload):
        """Speichert Twin-Daten in der Datenbank (synchroner ORM-Aufruf)."""
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

    @sync_to_async
    def _save_malfunction_log(self, payload):
        """Speichert eine Störmeldung (Info, Warning, Error) in der Datenbank (synchroner ORM-Aufruf)."""
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