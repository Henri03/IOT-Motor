# path: IOT_PROJECT/src/iot_app/management/commands/mqtt_consumer.py
import json
import paho.mqtt.client as mqtt
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from asgiref.sync import async_to_sync, sync_to_async
from channels.layers import get_channel_layer
from iot_app.models import MotorInfo, LiveData, TwinData, MalfunctionLog, RawData, FeatureData, PredictionData
import traceback
from datetime import datetime
import asyncio
import pytz
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Startet einen MQTT-Consumer, um Sensordaten zu empfangen und in der Datenbank zu speichern.'

    last_anomaly_state = {}

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
        parser.add_argument('--data_freshness_threshold', type=int, default=settings.DATA_FRESHNESS_THRESHOLD_SECONDS,
                            help='Maximale Zeit in Sekunden, nach der Daten als veraltet gelten und als "-" angezeigt werden.')
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
        self.broker_address = options['broker']
        self.port = options['port']
        self.topic_live = options['topic_live']
        self.topic_twin = options['topic_twin']
        self.topic_malfunction_info = options['topic_malfunction_info']
        self.topic_malfunction_warning = options['topic_malfunction_warning']
        self.topic_malfunction_error = options['topic_malfunction_error']
        self.deviation_threshold = options['deviation_threshold']
        self.data_freshness_threshold = options['data_freshness_threshold']

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
        metrics_to_compare = ['current', 'temp', 'torque']
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
        self.stdout.write(self.style.SUCCESS(f"Datenaktualitäts-Schwellenwert: {self.data_freshness_threshold} Sekunden"))

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.on_connect = async_to_sync(self._on_connect_async)
        client.on_message = async_to_sync(self._on_message_async)
        client.user_data_set({
            'command_instance': self
        })

        try:
            client.connect(self.broker_address, self.port, 60)
            client.loop_forever()
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"MQTT-Verbindungsfehler: {e}"))
            self.stderr.write(self.style.ERROR("Stellen Sie sicher, dass der MQTT-Broker (Mosquitto) läuft und erreichbar ist."))

    async def _on_connect_async(self, client, userdata, flags, rc, properties):
        """
        Callback-Funktion, die aufgerufen wird, wenn der Client eine Verbindung zum Broker herstellt.
        Abonniert die konfigurierten MQTT-Topics.
        """
        command_instance = userdata['command_instance']
        if rc == 0:
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

    async def _on_message_async(self, client, userdata, msg):
        """
        Callback-Funktion, die aufgerufen wird, wenn eine Nachricht auf einem abonnierten Topic empfangen wird.
        Leitet die Nachricht an den entsprechenden Handler weiter.
        """
        command_instance = userdata['command_instance']

        # Zentrales Logging für empfangene Nachrichten
        command_instance.stdout.write(f"Empfangen auf Topic: {msg.topic}, Payload: {msg.payload.decode()}")

        try:
            payload = json.loads(msg.payload.decode())
            logger.debug(f"Nachricht auf Topic {msg.topic} empfangen: {payload}")

            motor = await sync_to_async(MotorInfo.objects.first)()
            if not motor:
                command_instance.stderr.write(command_instance.style.ERROR("Kein Motor in der Datenbank gefunden. Bitte im Admin-Bereich erstellen."))
                return

            # --- Topic-basierte Nachrichtenverarbeitung ---
            if msg.topic == command_instance.topic_live:
                await command_instance._handle_live_data(msg.topic, payload)
            elif msg.topic == command_instance.topic_twin:
                await command_instance._handle_twin_data(msg.topic, payload)
            elif msg.topic == command_instance.topic_malfunction_info:
                await command_instance._handle_malfunction_data(msg.topic, payload, 'INFO')
            elif msg.topic == command_instance.topic_malfunction_warning:
                await command_instance._handle_malfunction_data(msg.topic, payload, 'WARNING')
            elif msg.topic == command_instance.topic_malfunction_error:
                await command_instance._handle_malfunction_data(msg.topic, payload, 'ERROR')
            elif msg.topic == command_instance.topic_raw_temperature:
                await command_instance._handle_raw_data(msg.topic, payload, 'temperature')
            elif msg.topic == command_instance.topic_raw_current:
                await command_instance._handle_raw_data(msg.topic, payload, 'current')
            elif msg.topic == command_instance.topic_raw_torque:
                await command_instance._handle_raw_data(msg.topic, payload, 'torque')
            elif msg.topic == command_instance.topic_feature_temperature:
                await command_instance._handle_feature_data(msg.topic, payload, 'temperature')
            elif msg.topic == command_instance.topic_feature_current:
                await command_instance._handle_feature_data(msg.topic, payload, 'current')
            elif msg.topic == command_instance.topic_feature_torque:
                await command_instance._handle_feature_data(msg.topic, payload, 'torque')
            elif msg.topic == command_instance.topic_prediction_temperature:
                await command_instance._handle_prediction_data(msg.topic, payload, 'temperature')
            elif msg.topic == command_instance.topic_prediction_current:
                await command_instance._handle_prediction_data(msg.topic, payload, 'current')
            elif msg.topic == command_instance.topic_prediction_torque:
                await command_instance._handle_prediction_data(msg.topic, payload, 'torque')
            else:
                command_instance.stdout.write(f"INFO: Unbekanntes Topic empfangen: {msg.topic}")

            # WICHTIG: _process_and_notify_dashboard() und _notify_dashboard_for_plot_data_point()
            # werden jetzt in den einzelnen _handle_... Methoden aufgerufen,
            # um sicherzustellen, dass sowohl Plots als auch Panel-Daten
            # bei jedem relevanten Datenpunkt aktualisiert werden.
            # Der Aufruf hier ist redundant und wird entfernt.

        except json.JSONDecodeError:
            command_instance.stderr.write(command_instance.style.ERROR(f"Fehler beim Dekodieren von JSON aus der Nachricht: {msg.payload} auf Topic {msg.topic}"))
        except Exception as e:
            command_instance.stderr.write(command_instance.style.ERROR(f"Ein unerwarteter Fehler bei der Verarbeitung der MQTT-Nachricht auf Topic {msg.topic}: {e}"))
            traceback.print_exc()

    async def _handle_live_data(self, topic, payload):
        """
        Verarbeitet Nachrichten vom Live-Daten-Topic.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Handling Live Data: Topic={topic}, Payload={payload}")
        await self._save_live_data(payload)
        logger.debug(f"Live-Daten gespeichert.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # LiveData ist die Hauptquelle für Panel-Werte, daher Panel-Update triggern
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_twin_data(self, topic, payload):
        """
        Verarbeitet Nachrichten vom Twin-Daten-Topic.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Handling Twin Data: Topic={topic}, Payload={payload}")
        await self._save_twin_data(payload)
        logger.debug(f"Twin-Daten gespeichert.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # TwinData ist wichtig für Panel-Vergleiche, daher Panel-Update triggern
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_malfunction_data(self, topic, payload, topic_type):
        """
        Verarbeitet Nachrichten von den Malfunction-Topics (Info, Warning, Error).
        Speichert die Daten im MalfunctionLog und benachrichtigt das Dashboard über eine Änderung im Anomaly-Status.
        """
        self.stdout.write(f"Handling Malfunction Data ({topic_type}): Topic={topic}, Payload={payload}")
        if 'message_type' not in payload:
            payload['message_type'] = topic_type
        await self._save_malfunction_log(payload)
        logger.warning(f"Störungsprotokoll ({topic_type}) gespeichert: {payload.get('description')}")
        # Malfunction logs beeinflussen den Anomaly-Status und die Log-Anzeige, daher Panel-Update triggern
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_raw_data(self, topic, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Rohdaten-Topics.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Handling Raw Data ({metric_type}): Topic={topic}, Payload={payload}")
        await self._save_raw_data(payload, metric_type)
        logger.debug(f"Rohdaten für {metric_type} empfangen und gespeichert: {payload}.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # Rohdaten beeinflussen die Panel-Anzeige des realen Motors, daher Panel-Update triggern
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_feature_data(self, topic, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Feature-Topics.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Handling Feature Data ({metric_type}): Topic={topic}, Payload={payload}")
        await self._save_feature_data(payload, metric_type)
        logger.debug(f"Feature-Daten für {metric_type} empfangen und gespeichert: {payload}.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # Feature-Daten können die Anomalie-Erkennung beeinflussen (wenn diese Features nutzt), daher Panel-Update triggern
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_prediction_data(self, topic, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Vorhersage-Topics.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Handling Prediction Data ({metric_type}): Topic={topic}, Payload={payload}")
        await self._save_prediction_data(payload, metric_type)
        logger.debug(f"Vorhersagedaten für {metric_type} empfangen und gespeichert: {payload}.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # Vorhersagedaten beeinflussen die Anomalie-Erkennung und die Plot-Darstellung, daher Panel-Update triggern
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _notify_dashboard_for_plot_data_point(self):
        """
        Asynchrone Hilfsfunktion zum Senden einer Nachricht an die Dashboard-Gruppe
        über den Channels Layer, um einen neuen Plot-Datenpunkt zu signalisieren.
        """
        channel_layer = get_channel_layer()
        if channel_layer:
            await channel_layer.group_send(
                "iot_dashboard_group",
                {
                    "type": "dashboard_message",
                    "message_type": "plot_data_point",
                    "data": {}
                }
            )
            logger.debug("Sent plot_data_point notification to channel layer.")
        else:
            logger.warning("Channel layer not available for plot_data_point notification.")

    async def _process_and_notify_dashboard(self, deviation_threshold, last_anomaly_state):
        """
        Ruft die neuesten Dashboard-relevanten Daten ab, führt Anomalie-Erkennung durch,
        erstellt bei Bedarf MalfunctionLogs und sendet ein umfassendes Update
        an alle verbundenen Dashboard-Clients über den Channels Layer.
        """
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.warning("Channel layer nicht verfügbar, Dashboard-Update übersprungen.")
            return

        # Kleine Verzögerung, um sicherzustellen, dass Daten in der DB sind und um den Event-Loop nicht zu überlasten
        await asyncio.sleep(0.05)

        motor = await sync_to_async(MotorInfo.objects.first)()
        if not motor:
            logger.warning("Kein Motor gefunden, kann kein detailliertes Dashboard-Update senden.")
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

        latest_raw_data = {
            'temperature': await sync_to_async(RawData.objects.filter(metric_type='temperature').order_by('-timestamp').first)(),
            'current': await sync_to_async(RawData.objects.filter(metric_type='current').order_by('-timestamp').first)(),
            'torque': await sync_to_async(RawData.objects.filter(metric_type='torque').order_by('-timestamp').first)(),
        }
        latest_feature_data = {
            'temperature': await sync_to_async(FeatureData.objects.filter(metric_type='temperature').order_by('-timestamp').first)(),
            'current': await sync_to_async(FeatureData.objects.filter(metric_type='current').order_by('-timestamp').first)(),
            'torque': await sync_to_async(FeatureData.objects.filter(metric_type='torque').order_by('-timestamp').first)(),
        }
        latest_prediction_data = {
            'temperature': await sync_to_async(PredictionData.objects.filter(metric_type='temperature').order_by('-timestamp').first)(),
            'current': await sync_to_async(PredictionData.objects.filter(metric_type='current').order_by('-timestamp').first)(),
            'torque': await sync_to_async(PredictionData.objects.filter(metric_type='torque').order_by('-timestamp').first)(),
        }

        def is_data_fresh(data_object, threshold_seconds):
            if data_object and data_object.timestamp:
                return (timezone.now() - data_object.timestamp).total_seconds() < threshold_seconds
            return False

        latest_malfunction_logs_for_display = await sync_to_async(list)(
            MalfunctionLog.objects.order_by('-timestamp')[:5].values()
        )

        real_motor_data = {
            "Strom": {"value": latest_raw_data['current'].value if is_data_fresh(latest_raw_data['current'], self.data_freshness_threshold) else '-', "unit": "A"},
            "Spannung": {"value": latest_live_data.voltage if is_data_fresh(latest_live_data, self.data_freshness_threshold) else '-', "unit": "V"},
            "Drehzahl": {"value": latest_live_data.rpm if is_data_fresh(latest_live_data, self.data_freshness_threshold) else '-', "unit": "U/min"},
            "Vibration": {"value": latest_live_data.vibration if is_data_fresh(latest_live_data, self.data_freshness_threshold) else '-', "unit": "mm/s"},
            "Temperatur": {"value": latest_raw_data['temperature'].value if is_data_fresh(latest_raw_data['temperature'], self.data_freshness_threshold) else '-', "unit": "°C"},
            "Drehmoment": {"value": latest_raw_data['torque'].value if is_data_fresh(latest_raw_data['torque'], self.data_freshness_threshold) else '-', "unit": "Nm"},
            "Laufzeit": {"value": latest_live_data.run_time if is_data_fresh(latest_live_data, self.data_freshness_threshold) else '-', "unit": "h"},
            "Anzahl eingefahren": {"value": retracted_count, "unit": ""},
            "Anzahl ausgefahren": {"value": extended_count, "unit": ""},
        }

        digital_twin_data = {
            "Strom": {"value": latest_twin_data.current if is_data_fresh(latest_twin_data, self.data_freshness_threshold) else None, "unit": "A"},
            "Spannung": {"value": latest_twin_data.voltage if is_data_fresh(latest_twin_data, self.data_freshness_threshold) else None, "unit": "V"},
            "Drehzahl": {"value": latest_twin_data.rpm if is_data_fresh(latest_twin_data, self.data_freshness_threshold) else None, "unit": "U/min"},
            "Vibration": {"value": latest_twin_data.vibration if is_data_fresh(latest_twin_data, self.data_freshness_threshold) else None, "unit": "mm/s"},
            "Temperatur": {"value": latest_twin_data.temp if is_data_fresh(latest_twin_data, self.data_freshness_threshold) else None, "unit": "°C"},
            "Drehmoment": {"value": latest_twin_data.torque if is_data_fresh(latest_twin_data, self.data_freshness_threshold) else None, "unit": "Nm"},
            "Laufzeit": {"value": latest_twin_data.run_time if is_data_fresh(latest_twin_data, self.data_freshness_threshold) else None, "unit": "h"},
            "Anzahl eingefahren": {"value": retracted_count, "unit": ""},
            "Anzahl ausgefahren": {"value": extended_count, "unit": ""},
        }

        dashboard_raw_data = {
            metric: {'value': getattr(data, 'value', None) if is_data_fresh(data, self.data_freshness_threshold) else None, 'timestamp': getattr(data, 'timestamp', None)}
            for metric, data in latest_raw_data.items()
        }
        dashboard_feature_data = {
            metric: {
                'mean': getattr(data, 'mean', None) if is_data_fresh(data, self.data_freshness_threshold) else None,
                'min': getattr(data, 'min_val', None) if is_data_fresh(data, self.data_freshness_threshold) else None,
                'max': getattr(data, 'max_val', None) if is_data_fresh(data, self.data_freshness_threshold) else None,
                'median': getattr(data, 'median', None) if is_data_fresh(data, self.data_freshness_threshold) else None,
                'std': getattr(data, 'std_dev', None) if is_data_fresh(data, self.data_freshness_threshold) else None,
                'range': getattr(data, 'data_range', None) if is_data_fresh(data, self.data_freshness_threshold) else None,
                'timestamp': getattr(data, 'timestamp', None)
            } for metric, data in latest_feature_data.items()
        }
        dashboard_prediction_data = {
            metric: {
                'status_value': getattr(data, 'status_value', None) if is_data_fresh(data, self.data_freshness_threshold) else None,
                'timestamp': getattr(data, 'timestamp', None)
            } for metric, data in latest_prediction_data.items()
        }
        current_anomaly_detected = False
        current_anomaly_message = "Motor läuft normal."

        metrics_to_compare = ['current', 'temp', 'torque']

        logs_to_create = []

        if not latest_raw_data['current'] or not latest_raw_data['temperature'] or not latest_raw_data['torque'] or not latest_twin_data or \
            not is_data_fresh(latest_raw_data['current'], self.data_freshness_threshold) or \
            not is_data_fresh(latest_raw_data['temperature'], self.data_freshness_threshold) or \
            not is_data_fresh(latest_raw_data['torque'], self.data_freshness_threshold) or \
            not is_data_fresh(latest_twin_data, self.data_freshness_threshold):
            current_anomaly_message = "Keine ausreichenden oder aktuellen Daten für Anomalie-Erkennung verfügbar."
            current_anomaly_detected = True

        else:
            for metric_name in metrics_to_compare:
                raw_data_obj = latest_raw_data.get(metric_name)
                raw_value = raw_data_obj.value if raw_data_obj else None

                twin_value = getattr(latest_twin_data, metric_name, None)

                is_currently_deviating = False
                if raw_value is not None and twin_value is not None:
                    if twin_value != 0:
                        deviation = abs((raw_value - twin_value) / twin_value) * 100
                        if deviation > deviation_threshold:
                            is_currently_deviating = True
                            current_anomaly_detected = True
                            if "KRITISCHE STÖRUNG" not in current_anomaly_message:
                                current_anomaly_message = f"WARNUNG: {metric_name.capitalize()}-Abweichung erkannt."
                    elif raw_value != 0 and twin_value == 0:
                        is_currently_deviating = True
                        current_anomaly_detected = True
                        if "KRITISCHE STÖRUNG" not in current_anomaly_message:
                            current_anomaly_message = f"WARNUNG: {metric_name.capitalize()}-Abweichung: Twin ist 0, Raw ist {raw_value:.2f}."

                if is_currently_deviating and not last_anomaly_state.get(metric_name, False):
                    logs_to_create.append({
                        'message_type': 'WARNING',
                        'description': f"{metric_name.capitalize()}-Abweichung > {deviation_threshold:.1f}% erkannt (Raw: {raw_value:.2f}, Twin: {twin_value:.2f})",
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

            unacknowledged_error_logs = await sync_to_async(list)(MalfunctionLog.objects.filter(
                timestamp__gte=timezone.now() - timezone.timedelta(minutes=5),
                message_type='ERROR',
                acknowledged=False
            ).order_by('-timestamp').values())

            if unacknowledged_error_logs:
                current_anomaly_detected = True
                current_anomaly_message = f"KRITISCHE STÖRUNG: {unacknowledged_error_logs[0]['description']}"
            else:
                unacknowledged_warning_logs = await sync_to_async(list)(MalfunctionLog.objects.filter(
                    timestamp__gte=timezone.now() - timezone.timedelta(minutes=5),
                    message_type='WARNING',
                    acknowledged=False
                ).order_by('-timestamp').values())

                if unacknowledged_warning_logs:
                    current_anomaly_detected = True
                    current_anomaly_message = f"WARNUNG: {unacknowledged_warning_logs[0]['description']}"

        rounded_real_motor_data = _round_numeric_values_for_display(real_motor_data, decimal_places=2)
        rounded_digital_twin_data = _round_numeric_values_for_display(digital_twin_data, decimal_places=2)
        rounded_dashboard_raw_data = _round_numeric_values_for_display(dashboard_raw_data, decimal_places=2)
        rounded_dashboard_feature_data = _round_numeric_values_for_display(dashboard_feature_data, decimal_places=2)
        rounded_dashboard_prediction_data = _round_numeric_values_for_display(dashboard_prediction_data, decimal_places=2)

        message_to_send = {
            'real_motor_data': rounded_real_motor_data,
            'digital_twin_data': rounded_digital_twin_data,
            'anomaly_status': {
                'detected': current_anomaly_detected,
                'message': current_anomaly_message
            },
            'malfunction_logs': latest_malfunction_logs_for_display,
            'raw_data': rounded_dashboard_raw_data,
            'feature_data': rounded_dashboard_feature_data,
            'prediction_data': rounded_dashboard_prediction_data,
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
            logger.debug("Sent dashboard_update to channel layer.")
        except Exception as e:
            logger.error(f"Fehler beim Senden des Dashboard-Updates an Channel Layer: {e}")
            traceback.print_exc()

    @sync_to_async
    def _save_live_data(self, payload):
        """
        Speichert Live-Daten in der Datenbank.
        Uses payload timestamp if available and makes it timezone-aware, otherwise uses timezone.now().
        """
        timestamp = make_aware_from_iso(payload.get('timestamp')) if 'timestamp' in payload else timezone.now()
        LiveData.objects.create(
            timestamp=timestamp,
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
        """
        Speichert Twin-Daten in der Datenbank.
        Uses payload timestamp if available and makes it timezone-aware, otherwise uses timezone.now().
        """
        timestamp = make_aware_from_iso(payload.get('timestamp')) if 'timestamp' in payload else timezone.now()
        TwinData.objects.create(
            timestamp=timestamp,
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
        """
        Speichert eine Störmeldung (Info, Warning, Error) in der Datenbank.
        Uses payload timestamp if available and makes it timezone-aware, otherwise uses timezone.now().
        """
        timestamp = make_aware_from_iso(payload.get('timestamp')) if 'timestamp' in payload else timezone.now()
        MalfunctionLog.objects.create(
            timestamp=timestamp,
            message_type=payload.get('message_type'),
            description=payload.get('description'),
            motor_state=payload.get('motor_state', 'unbekannt'),
            emergency_stop_active=payload.get('emergency_stop_active', False),
        )

    @sync_to_async
    def _save_raw_data(self, payload, metric_type):
        """
        Speichert Rohdaten in der Datenbank.
        Macht den Timestamp timezone-aware.
        """
        timestamp = make_aware_from_iso(payload.get('timestamp'))
        RawData.objects.create(
            timestamp=timestamp,
            metric_type=metric_type,
            value=payload.get('value')
        )

    @sync_to_async
    def _save_feature_data(self, payload, metric_type):
        """
        Speichert Feature-Daten in der Datenbank.
        Macht den Timestamp timezone-aware.
        """
        timestamp = make_aware_from_iso(payload.get('timestamp'))
        FeatureData.objects.create(
            timestamp=timestamp,
            metric_type=metric_type,
            mean=payload.get('mean'),
            min_val=payload.get('min'),
            max_val=payload.get('max'),
            median=payload.get('median'),
            std_dev=payload.get('std'),
            data_range=payload.get('range'),
        )

    @sync_to_async
    def _save_prediction_data(self, payload, metric_type):
        """
        Speichert Vorhersagedaten in der Datenbank.
        Macht den Timestamp timezone-aware.
        """
        timestamp = make_aware_from_iso(payload.get('timestamp'))
        PredictionData.objects.create(
            timestamp=timestamp,
            metric_type=metric_type,
            status_value=payload.get('value'),
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

    # Utility function to make naive datetimes timezone-aware
def make_aware_from_iso(iso_string):
    """
    Parses an ISO formatted string to a datetime object and ensures it's timezone-aware (UTC).
    If the string already contains timezone info, it will be parsed as aware.
    If it's naive, it will be made aware with UTC.
    """
    if not iso_string:
        return None
    dt_object = datetime.fromisoformat(iso_string)
    if timezone.is_aware(dt_object):
        # If the datetime object is already timezone-aware, ensure it's in UTC
        return dt_object.astimezone(pytz.utc)
    else:
        # If it's naive, make it timezone-aware with UTC
        return timezone.make_aware(dt_object, pytz.utc)

def _round_numeric_values_for_display(data_dict, decimal_places=2):
    """
    Recursively rounds numeric 'value' fields within a dictionary for display purposes.
    This function creates a deep copy to avoid modifying the original data.
    It also handles the new structure of feature and prediction data.
    """
    rounded_data = {}
    for key, item in data_dict.items():
        if isinstance(item, dict):
            if 'value' in item and isinstance(item['value'], (int, float)):
                rounded_data[key] = {**item, 'value': round(item['value'], decimal_places)}
            elif 'mean' in item and 'min' in item:
                rounded_item = {}
                for sub_key, sub_value in item.items():
                    if isinstance(sub_value, (int, float)):
                        rounded_item[sub_key] = round(sub_value, decimal_places)
                    else:
                        rounded_item[sub_key] = sub_value
                rounded_data[key] = rounded_item
            elif 'status_value' in item:
                rounded_item = {**item}
                rounded_data[key] = rounded_item
            else:
                rounded_data[key] = _round_numeric_values_for_display(item, decimal_places)
        elif isinstance(item, list):
            rounded_data[key] = [_round_numeric_values_for_display(elem, decimal_places) for elem in item]
        else:
            rounded_data[key] = item
    return rounded_data