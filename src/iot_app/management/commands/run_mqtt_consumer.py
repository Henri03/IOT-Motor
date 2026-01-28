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

    # Dictionary zur Speicherung des letzten Anomaliezustands für jede Metrik (für abweichungsbasierte Anomalien)
    last_anomaly_state = {}
    # Dictionary zur Speicherung von Zählern für aufeinanderfolgende "schlechte" Vorhersagewerte
    prediction_error_counts = {
        'temperature': 0,
        'current': 0,
        'torque': 0,
    }
    # Flag, das anzeigt, ob für eine Metrik derzeit ein FEHLER-Zustand aktiv ist
    error_active_flags = {
        'temperature': False,
        'current': False,
        'torque': False,
    }
    # Schwellenwert für die Anzahl der aufeinanderfolgenden schlechten Vorhersagen, bevor ein FEHLER ausgelöst wird
    PREDICTION_ERROR_THRESHOLD = 10

    def add_arguments(self, parser):
        """
        Definiert die Kommandozeilenargumente für den MQTT-Consumer.
        """
        parser.add_argument('--broker', type=str, default=settings.DOCKER_MQTT_BROKER_HOST, help='MQTT Broker Host (intern für Docker)')
        parser.add_argument('--port', type=int, default=settings.MQTT_BROKER_PORT, help='MQTT Broker Port')
        parser.add_argument('--topic_live', type=str, default=settings.MQTT_TOPIC_LIVE, help='MQTT Topic für Live-Daten')
        parser.add_argument('--topic_twin', type=str, default=settings.MQTT_TOPIC_TWIN, help='MQTT Topic für Twin-Daten')
        parser.add_argument('--topic_malfunction_info', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_INFO, help='MQTT Topic für Info-Nachrichten')
        parser.add_argument('--topic_malfunction_warning', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_WARNING, help='MQTT Topic für Warnmeldungen')
        parser.add_argument('--topic_malfunction_error', type=str, default=settings.MQTT_TOPIC_MALFUNCTION_ERROR, help='MQTT Topic für Fehlermeldungen')
        parser.add_argument('--deviation_threshold', type=float, default=getattr(settings, 'MQTT_DEVIATION_THRESHOLD_PERCENT', 10.0),
                            help='Prozentuale Abweichung zwischen Live- und Twin-Daten, die eine Warnung auslöst.')
        parser.add_argument('--data_freshness_threshold', type=int, default=settings.DATA_FRESHNESS_THRESHOLD_SECONDS,
                            help='Maximale Zeit in Sekunden, nach der Daten als veraltet gelten und als "-" angezeigt werden.')
        parser.add_argument('--topic_raw_temperature', type=str, default=getattr(settings, 'MQTT_TOPIC_RAW_TEMPERATURE', "raw/temperature"), help='MQTT Topic für Rohdaten Temperatur')
        parser.add_argument('--topic_raw_current', type=str, default=getattr(settings, 'MQTT_TOPIC_RAW_CURRENT', "raw/current"), help='MQTT Topic für Rohdaten Strom')
        parser.add_argument('--topic_raw_torque', type=str, default=getattr(settings, 'MQTT_TOPIC_RAW_TORQUE', "raw/torque"), help='MQTT Topic für Rohdaten Drehmoment')

        parser.add_argument('--topic_feature_temperature', type=str, default=getattr(settings, 'MQTT_TOPIC_FEATURE_TEMPERATURE', "feature/temperature"), help='MQTT Topic für Feature-Daten Temperatur')
        parser.add_argument('--topic_feature_current', type=str, default=getattr(settings, 'MQTT_TOPIC_FEATURE_CURRENT', "feature/current"), help='MQTT Topic für Feature-Daten Strom')
        parser.add_argument('--topic_feature_torque', type=str, default=getattr(settings, 'MQTT_TOPIC_FEATURE_TORQUE', "feature/torque"), help='MQTT Topic für Feature-Daten Drehmoment')

        parser.add_argument('--topic_prediction_temperature', type=str, default=getattr(settings, 'MQTT_TOPIC_PREDICTION_TEMPERATURE', "prediction/temperature"), help='MQTT Topic für Vorhersage-Daten Temperatur')
        parser.add_argument('--topic_prediction_current', type=str, default=getattr(settings, 'MQTT_TOPIC_PREDICTION_CURRENT', "prediction/current"), help='MQTT Topic für Vorhersage-Daten Strom')
        parser.add_argument('--topic_prediction_torque', type=str, default=getattr(settings, 'MQTT_TOPIC_PREDICTION_TORQUE', "prediction/torque"), help='MQTT Topic für Vorhersage-Daten Drehmoment')

    def handle(self, *args, **options):
        """
        Die Hauptmethode, die ausgeführt wird, wenn der Management-Befehl aufgerufen wird.
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
            # Initialisiert das Fehler-Aktiv-Flag für Vorhersagen
            self.error_active_flags[metric_name] = False 

        self.stdout.write(self.style.SUCCESS(f"Starte MQTT-Consumer für den Motor"))
        self.stdout.write(self.style.SUCCESS(f"Verbinde mit MQTT-Broker: {self.broker_address}:{self.port}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Live-Daten-Topic: {self.topic_live}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Twin-Daten-Topic: {self.topic_twin}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Störungsinformationen-Topic: {self.topic_malfunction_info}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Störungswarnung-Topic: {self.topic_malfunction_warning}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Störungsfehler-Topic: {self.topic_malfunction_error}"))

        self.stdout.write(self.style.SUCCESS(f"Abonniere Rohdaten-Topics: {self.topic_raw_temperature}, {self.topic_raw_current}, {self.topic_raw_torque}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Feature-Topics: {self.topic_feature_temperature}, {self.topic_feature_current}, {self.topic_feature_torque}"))
        self.stdout.write(self.style.SUCCESS(f"Abonniere Vorhersage-Topics: {self.topic_prediction_temperature}, {self.topic_prediction_current}, {self.topic_prediction_torque}"))

        self.stdout.write(self.style.SUCCESS(f"Abweichungsschwellenwert für Warnungen: {self.deviation_threshold}%"))
        self.stdout.write(self.style.SUCCESS(f"Datensatz-Frische-Schwellenwert: {self.data_freshness_threshold} Sekunden"))

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
            self.stderr.write(self.style.ERROR("Bitte stellen Sie sicher, dass der MQTT-Broker (Mosquitto) läuft und erreichbar ist."))

    async def _on_connect_async(self, client, userdata, flags, rc, properties):
        """
        Callback-Funktion, die aufgerufen wird, wenn der Client eine Verbindung zum Broker herstellt.
        Abonniert die konfigurierten MQTT-Topics.
        """
        command_instance = userdata['command_instance']
        if rc == 0:
            command_instance.stdout.write(command_instance.style.SUCCESS("Verbunden mit MQTT-Broker!"))
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

        # Zentralisierte Protokollierung für empfangene Nachrichten
        command_instance.stdout.write(f"Empfangen auf Topic: {msg.topic}, Payload: {msg.payload.decode()}")

        try:
            payload = json.loads(msg.payload.decode())
            logger.debug(f"Nachricht empfangen auf Topic {msg.topic}: {payload}")

            motor = await sync_to_async(MotorInfo.objects.first)()
            if not motor:
                command_instance.stderr.write(command_instance.style.ERROR("Kein Motor in der Datenbank gefunden. Bitte erstellen Sie einen im Admin-Bereich."))
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
            # werden jetzt innerhalb der einzelnen _handle_... Methoden aufgerufen,
            # um sicherzustellen, dass sowohl Plots als auch Panel-Daten
            # mit jedem relevanten Datenpunkt aktualisiert werden.
            # Der Aufruf hier ist redundant und wird entfernt.

        except json.JSONDecodeError:
            command_instance.stderr.write(command_instance.style.ERROR(f"Fehler beim Dekodieren von JSON aus der Nachricht: {msg.payload} auf Topic {msg.topic}"))
        except Exception as e:
            command_instance.stderr.write(command_instance.style.ERROR(f"Ein unerwarteter Fehler ist bei der Verarbeitung der MQTT-Nachricht auf Topic {msg.topic} aufgetreten: {e}"))
            traceback.print_exc()

    async def _handle_live_data(self, topic, payload):
        """
        Verarbeitet Nachrichten vom Live-Daten-Topic.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Verarbeite Live-Daten: Topic={topic}, Payload={payload}")
        await self._save_live_data(payload)
        logger.debug(f"Live-Daten gespeichert.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # LiveData ist die Hauptquelle für Panel-Werte, daher Panel-Update auslösen
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_twin_data(self, topic, payload):
        """
        Verarbeitet Nachrichten vom Twin-Daten-Topic.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Verarbeite Twin-Daten: Topic={topic}, Payload={payload}")
        await self._save_twin_data(payload)
        logger.debug(f"Twin-Daten gespeichert.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # TwinData ist wichtig für Panel-Vergleiche, daher Panel-Update auslösen
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_malfunction_data(self, topic, payload, topic_type):
        """
        Verarbeitet Nachrichten von den Störungs-Topics (Info, Warnung, Fehler).
        Speichert die Daten im MalfunctionLog und benachrichtigt das Dashboard über eine Änderung des Anomalie-Status.
        """
        self.stdout.write(f"Verarbeite Störungsdaten ({topic_type}): Topic={topic}, Payload={payload}")
        if 'message_type' not in payload:
            payload['message_type'] = topic_type
        await self._save_malfunction_log(payload)
        logger.warning(f"Störungsprotokoll ({topic_type}) gespeichert: {payload.get('description')}")
        # Störungsprotokolle beeinflussen den Anomalie-Status und die Protokollanzeige, daher Panel-Update auslösen
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_raw_data(self, topic, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Rohdaten-Topics.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Verarbeite Rohdaten ({metric_type}): Topic={topic}, Payload={payload}")
        await self._save_raw_data(payload, metric_type)
        logger.debug(f"Rohdaten für {metric_type} empfangen und gespeichert: {payload}.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # Rohdaten beeinflussen die Panel-Anzeige des realen Motors, daher Panel-Update auslösen
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_feature_data(self, topic, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Feature-Topics.
        Speichert die Daten und benachrichtigt das Dashboard über einen neuen Plot-Datenpunkt.
        """
        self.stdout.write(f"Verarbeite Feature-Daten ({metric_type}): Topic={topic}, Payload={payload}")
        await self._save_feature_data(payload, metric_type)
        logger.debug(f"Feature-Daten für {metric_type} empfangen und gespeichert: {payload}.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # Feature-Daten können die Anomalieerkennung beeinflussen (wenn sie diese Features verwendet), daher Panel-Update auslösen
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _handle_prediction_data(self, topic, payload, metric_type):
        """
        Verarbeitet Nachrichten von den Vorhersage-Topics.
        Speichert die Daten und löst bei Bedarf Warnungen/Fehler basierend auf dem Vorhersagewert aus.
        """
        self.stdout.write(f"Verarbeite Vorhersage-Daten ({metric_type}): Topic={topic}, Payload={payload}")
        prediction_value = payload.get('value')

        # Extrahiere Zeitstempel für MalfunctionLog oder verwende aktuelle Zeit
        # Sicherstellen, dass make_aware_from_iso nur mit einem String aufgerufen wird
        timestamp_str = payload.get('timestamp')
        log_timestamp = make_aware_from_iso(timestamp_str) if isinstance(timestamp_str, str) else timezone.now()

        if prediction_value == -1:
            # Wenn bereits ein FEHLER für diese Metrik aktiv ist, keinen weiteren FEHLER protokollieren
            if self.error_active_flags[metric_type]:
                logger.debug(f"FEHLER für {metric_type.capitalize()} ist bereits aktiv. Überspringe neues FEHLER-Protokoll.")
            else:
                self.prediction_error_counts[metric_type] += 1
                if self.prediction_error_counts[metric_type] >= self.PREDICTION_ERROR_THRESHOLD:
                    # Fehler nach Erreichen des Schwellenwerts
                    description = f"{metric_type.capitalize()}-Vorhersage ist schlecht (-1) {self.PREDICTION_ERROR_THRESHOLD} Mal in Folge."
                    await self._save_malfunction_log({
                        'message_type': 'ERROR',
                        'description': description,
                        'motor_state': 'fehlerhaft',
                        'emergency_stop_active': True,
                        'timestamp': log_timestamp
                    })
                    self.stdout.write(self.style.ERROR(description))
                    self.error_active_flags[metric_type] = True # Fehler als aktiv setzen
                    self.prediction_error_counts[metric_type] = 0 # Zähler nach Auslösen des FEHLERS zurücksetzen
                else:
                    # Warnung für jeden schlechten Vorhersagewert, bevor der Schwellenwert erreicht ist
                    description = f"{metric_type.capitalize()}-Vorhersage ist schlecht (-1). Aufeinanderfolgende schlechte Vorhersagen: {self.prediction_error_counts[metric_type]}/{self.PREDICTION_ERROR_THRESHOLD}."
                    await self._save_malfunction_log({
                        'message_type': 'WARNING',
                        'description': description,
                        'motor_state': 'potenziell fehlerhaft',
                        'emergency_stop_active': False,
                        'timestamp': log_timestamp
                    })
                    self.stdout.write(self.style.WARNING(description))
        else:
            # Wenn der Vorhersagewert nicht -1 ist, prüfen, ob eine INFO-Nachricht protokolliert und Zustände zurückgesetzt werden müssen
            # Eine INFO-Nachricht soll nur gesendet werden, wenn zuvor ein Fehler oder eine Warnung aktiv war
            if self.prediction_error_counts[metric_type] > 0 or self.error_active_flags[metric_type]:
                # Wenn ein Fehler oder eine Warnung behoben wurde, soll das Anomaliefeld wieder "Motor läuft normal" anzeigen
                description = f"INFO: {metric_type.capitalize()}-Anomalie behoben. Vorhersage ist wieder normal."

                await self._save_malfunction_log({
                    'message_type': 'INFO',
                    'description': description,
                    'motor_state': 'normal', # Setze den Motorzustand auf 'normal'
                    'emergency_stop_active': False,
                    'timestamp': log_timestamp
                })
                self.stdout.write(self.style.NOTICE(description))
            self.prediction_error_counts[metric_type] = 0
            self.error_active_flags[metric_type] = False # Aktives Fehler-Flag löschen

        await self._save_prediction_data(payload, metric_type)
        logger.debug(f"Vorhersage-Daten für {metric_type} empfangen und gespeichert: {payload}.")
        await self._notify_dashboard_for_plot_data_point() # Benachrichtigt für Plots
        # Vorhersage-Daten beeinflussen die Anomalieerkennung und Plot-Anzeige, daher Panel-Update auslösen
        await self._process_and_notify_dashboard(self.deviation_threshold, self.last_anomaly_state)

    async def _notify_dashboard_for_plot_data_point(self):
        """
        Asynchrone Hilfsfunktion zum Senden einer Nachricht an die Dashboard-Gruppe
        über die Channels-Schicht, die einen neuen Plot-Datenpunkt signalisiert.
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
            logger.debug("Plot_data_point-Benachrichtigung an Channel-Schicht gesendet.")
        else:
            logger.warning("Channel-Schicht nicht verfügbar für plot_data_point-Benachrichtigung.")

    async def _process_and_notify_dashboard(self, deviation_threshold, last_anomaly_state):
        """
        Ruft die neuesten Dashboard-relevanten Daten ab, führt eine Anomalieerkennung durch,
        erstellt bei Bedarf MalfunctionLogs und sendet ein umfassendes Update
        an alle verbundenen Dashboard-Clients über die Channels-Schicht.
        """
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.warning("Channel-Schicht nicht verfügbar, Dashboard-Update übersprungen.")
            return

        # Kleine Verzögerung, um sicherzustellen, dass die Daten in der DB sind und um eine Überlastung der Ereignisschleife zu vermeiden
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

        # Nur unbestätigte Warnungen/Fehler für die Anzeige im Dashboard abrufen
        latest_unacknowledged_malfunction_logs = await sync_to_async(list)(
            MalfunctionLog.objects.filter(acknowledged=False)
            .exclude(message_type='INFO') # INFO-Nachrichten nicht anzeigen
            .order_by('-timestamp')[:5].values()
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

        # Initialisiere den Anomalie-Status und die Nachricht
        current_anomaly_detected = False
        current_anomaly_message = "Motor läuft normal."
        
        # Flags, um festzuhalten, ob eine Anomalie eines bestimmten Typs aktiv ist
        deviation_anomaly_active = False
        prediction_anomaly_active = False

        metrics_to_compare = ['current', 'temperature', 'torque'] # 'temp' in TwinData, 'temperature' in RawData/PredictionData
        logs_to_create = []

        # --- 1. Überprüfen auf unzureichende oder veraltete Daten (höchste Priorität, da keine Erkennung möglich) ---
        if not latest_raw_data['current'] or not latest_raw_data['temperature'] or not latest_raw_data['torque'] or not latest_twin_data or \
            not is_data_fresh(latest_raw_data['current'], self.data_freshness_threshold) or \
            not is_data_fresh(latest_raw_data['temperature'], self.data_freshness_threshold) or \
            not is_data_fresh(latest_raw_data['torque'], self.data_freshness_threshold) or \
            not is_data_fresh(latest_twin_data, self.data_freshness_threshold):
            current_anomaly_detected = True
            current_anomaly_message = "Unzureichende oder veraltete Daten für die Anomalieerkennung verfügbar."
        else:
            # --- 2. Abweichungsbasierte Anomalieerkennung ---
            for metric_name in metrics_to_compare:
                raw_data_obj = latest_raw_data.get(metric_name)
                # 'temp' ist der Attributname in TwinData für Temperatur
                twin_metric_attr = 'temp' if metric_name == 'temperature' else metric_name
                twin_value = getattr(latest_twin_data, twin_metric_attr, None)
                raw_value = raw_data_obj.value if raw_data_obj else None

                is_currently_deviating = False
                if raw_value is not None and twin_value is not None:
                    if twin_value != 0:
                        deviation = abs((raw_value - twin_value) / twin_value) * 100
                        if deviation > deviation_threshold:
                            is_currently_deviating = True
                    elif raw_value != 0 and twin_value == 0: # Sonderfall: Twin ist 0, Raw ist nicht
                        is_currently_deviating = True
                
                if is_currently_deviating:
                    deviation_anomaly_active = True
                    if not last_anomaly_state.get(metric_name, False):
                        logs_to_create.append({
                            'message_type': 'WARNING',
                            'description': f"{metric_name.capitalize()}-Abweichung > {deviation_threshold:.1f}% erkannt (Raw: {raw_value:.2f}, Twin: {twin_value:.2f})",
                            'motor_state': 'unbekannt',
                            'emergency_stop_active': False,
                            'timestamp': timezone.now()
                        })
                        last_anomaly_state[metric_name] = True
                elif not is_currently_deviating and last_anomaly_state.get(metric_name, False):
                    # Wenn Abweichung behoben, logge INFO und setze Zustand zurück
                    logs_to_create.append({
                        'message_type': 'INFO',
                        'description': f"{metric_name.capitalize()}-Abweichung behoben. Motor läuft normal.",
                        'motor_state': 'normal',
                        'emergency_stop_active': False,
                        'timestamp': timezone.now()
                    })
                    last_anomaly_state[metric_name] = False

            # --- 3. Überprüfen auf aktive Vorhersagefehler (self.error_active_flags und self.prediction_error_counts) ---
            # Eine Vorhersage-Anomalie ist aktiv, wenn ein error_active_flag gesetzt ist ODER
            # wenn ein prediction_error_count > 0 ist, was eine laufende Warnung bedeutet
            if any(self.error_active_flags.values()) or any(count > 0 for count in self.prediction_error_counts.values()):
                prediction_anomaly_active = True

            # --- 4. Überprüfen auf unbestätigte Fehler- oder Warnmeldungen aus MalfunctionLog ---
            # Wir suchen nach den neuesten, unbestätigten ERROR- oder WARNING-Logs,
            # die NICHT von der Vorhersage stammen oder deren Ursache noch aktiv ist.
            
            # Zuerst alle unquittierten ERRORs und WARNINGs holen
            unacknowledged_errors_warnings = await sync_to_async(list)(
                MalfunctionLog.objects.filter(
                    acknowledged=False,
                    message_type__in=['ERROR', 'WARNING']
                ).order_by('-timestamp')
            )

            # Filtern, welche dieser Meldungen die Anomalie-Nachricht blockieren sollen
            # Eine Vorhersage-Warnung/Fehler wird ignoriert, wenn prediction_anomaly_active False ist.
            active_malfunction_log_message = None
            for log_entry in unacknowledged_errors_warnings:
                # Prüfen, ob es sich um eine Vorhersage-Anomalie handelt
                is_prediction_related = "Vorhersage ist schlecht" in log_entry.description or \
                                        "Vorhersage ist wieder normal" in log_entry.description # INFO-Meldungen werden eh ausgeschlossen

                if is_prediction_related:
                    # Wenn die Vorhersage-Anomalie technisch behoben ist (prediction_anomaly_active ist False),
                    # dann ignorieren wir diese spezifische MalfunctionLog-Meldung für das Haupt-Anomalie-Feld.
                    if not prediction_anomaly_active:
                        continue # Diese Meldung blockiert den "Motor läuft normal"-Status nicht
                
                # Wenn es keine Vorhersage-Anomalie ist ODER die Vorhersage-Anomalie noch aktiv ist,
                # dann ist dies eine relevante Meldung.
                active_malfunction_log_message = log_entry
                break # Die neueste relevante Meldung reicht

            # --- 5. Finaler Anomalie-Status und Nachricht basierend auf Priorität ---
            if current_anomaly_message == "Unzureichende oder veraltete Daten für die Anomalieerkennung verfügbar.":
                # Wenn Daten veraltet sind, bleibt diese Nachricht.
                pass
            elif active_malfunction_log_message:
                current_anomaly_detected = True
                if active_malfunction_log_message.message_type == 'ERROR':
                    current_anomaly_message = f"KRITISCHE STÖRUNG: {active_malfunction_log_message.description}"
                else: # WARNING
                    current_anomaly_message = f"WARNUNG: {active_malfunction_log_message.description}"
            elif deviation_anomaly_active:
                current_anomaly_detected = True
                current_anomaly_message = "WARNUNG: Abweichung zwischen Live- und Twin-Daten erkannt."
            elif prediction_anomaly_active: # Dies wird nur erreicht, wenn active_malfunction_log_message keine aktive Vorhersage-Meldung war (z.B. weil sie behoben wurde)
                current_anomaly_detected = True
                current_anomaly_message = "WARNUNG: Vorhersage-Anomalie erkannt."
            # Wenn keine der oben genannten Anomalien aktiv ist, ist der Motor normal
            else:
                current_anomaly_detected = False # Explizit auf False setzen
                current_anomaly_message = "Motor läuft normal."

        # Speichere alle gesammelten Logs
        for log_data in logs_to_create:
            await sync_to_async(MalfunctionLog.objects.create)(**log_data)

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
            'malfunction_logs': latest_unacknowledged_malfunction_logs, # Nur unquittierte Logs
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
            logger.debug("Dashboard-Update an Channel-Schicht gesendet.")
        except Exception as e:
            logger.error(f"Fehler beim Senden des Dashboard-Updates an die Channel-Schicht: {e}")
            traceback.print_exc()

    @sync_to_async
    def _save_live_data(self, payload):
        """
        Speichert Live-Daten in der Datenbank.
        Verwendet den Zeitstempel der Payload, falls verfügbar, und macht ihn zeitzonenbewusst,
        andernfalls wird timezone.now() verwendet.
        """
        timestamp_str = payload.get('timestamp')
        timestamp = make_aware_from_iso(timestamp_str) if isinstance(timestamp_str, str) else timezone.now()
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
        Verwendet den Zeitstempel der Payload, falls verfügbar, und macht ihn zeitzonenbewusst,
        andernfalls wird timezone.now() verwendet.
        """
        timestamp_str = payload.get('timestamp')
        timestamp = make_aware_from_iso(timestamp_str) if isinstance(timestamp_str, str) else timezone.now()
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
        Speichert eine Störungsmeldung (Info, Warnung, Fehler) in der Datenbank.
        Verwendet den Zeitstempel der Payload, falls verfügbar, und macht ihn zeitzonenbewusst,
        andernfalls wird timezone.now() verwendet.
        """
        timestamp_str = payload.get('timestamp')
        timestamp = make_aware_from_iso(timestamp_str) if isinstance(timestamp_str, str) else timezone.now()
        MalfunctionLog.objects.create(
            timestamp=timestamp,
            message_type=payload.get('message_type'),
            description=payload.get('description'),
            motor_state=payload.get('motor_state', 'unknown'),
            emergency_stop_active=payload.get('emergency_stop_active', False),
        )

    @sync_to_async
    def _save_raw_data(self, payload, metric_type):
        """
        Speichert Rohdaten in der Datenbank.
        Macht den Zeitstempel zeitzonenbewusst.
        """
        timestamp_str = payload.get('timestamp')
        timestamp = make_aware_from_iso(timestamp_str) if isinstance(timestamp_str, str) else timezone.now()
        RawData.objects.create(
            timestamp=timestamp,
            metric_type=metric_type,
            value=payload.get('value')
        )

    @sync_to_async
    def _save_feature_data(self, payload, metric_type):
        """
        Speichert Feature-Daten in der Datenbank.
        Macht den Zeitstempel zeitzonenbewusst.
        """
        timestamp_str = payload.get('timestamp')
        timestamp = make_aware_from_iso(timestamp_str) if isinstance(timestamp_str, str) else timezone.now()
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
        Speichert Vorhersage-Daten in der Datenbank.
        Macht den Zeitstempel zeitzonenbewusst.
        """
        timestamp_str = payload.get('timestamp')
        timestamp = make_aware_from_iso(timestamp_str) if isinstance(timestamp_str, str) else timezone.now()
        PredictionData.objects.create(
            timestamp=timestamp,
            metric_type=metric_type,
            status_value=payload.get('value'),
        )

# Globale Hilfsfunktion für die Serialisierung
def _to_serializable_dict(obj):
    """
    Rekursive Hilfsfunktion, die Datetime-Objekte in einem Dictionary/einer Liste
    in ISO 8601-Strings umwandelt, damit sie über Channels gesendet werden können.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_serializable_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_serializable_dict(elem) for elem in obj]
    return obj

# Hilfsfunktion, um naive Datetimes zeitzonenbewusst zu machen
def make_aware_from_iso(iso_string):
    """
    Analysiert einen ISO-formatierten String in ein Datetime-Objekt und stellt sicher,
    dass es zeitzonenbewusst (UTC) ist.
    Wenn der String bereits Zeitzoneninformationen enthält, wird er als zeitzonenbewusst analysiert.
    Wenn er naiv ist, wird er mit UTC zeitzonenbewusst gemacht.
    Gibt None zurück, wenn iso_string None oder leer ist.
    """
    if not iso_string:
        return None
    dt_object = datetime.fromisoformat(iso_string)
    if timezone.is_aware(dt_object):
        # Wenn das Datetime-Objekt bereits zeitzonenbewusst ist, stellen Sie sicher, dass es in UTC ist
        return dt_object.astimezone(pytz.utc)
    else:
        # Wenn es naiv ist, machen Sie es mit UTC zeitzonenbewusst
        return timezone.make_aware(dt_object, pytz.utc)

def _round_numeric_values_for_display(data_dict, decimal_places=2):
    """
    Rundet rekursiv numerische 'value'-Felder innerhalb eines Dictionaries für Anzeigezwecke.
    Diese Funktion erstellt eine tiefe Kopie, um die ursprünglichen Daten nicht zu ändern.
    Sie behandelt auch die neue Struktur von Feature- und Vorhersagedaten.
    """
    rounded_data = {}
    for key, item in data_dict.items():
        if isinstance(item, dict):
            if 'value' in item and isinstance(item['value'], (int, float)):
                rounded_data[key] = {**item, 'value': round(item['value'], decimal_places)}
            elif 'mean' in item and 'min' in item: # Annahme Feature-Datenstruktur
                rounded_item = {}
                for sub_key, sub_value in item.items():
                    if isinstance(sub_value, (int, float)):
                        rounded_item[sub_key] = round(sub_value, decimal_places)
                    else:
                        rounded_item[sub_key] = sub_value
                rounded_data[key] = rounded_item
            elif 'status_value' in item: # Annahme Vorhersage-Datenstruktur
                rounded_item = {**item}
                # status_value selbst wird nicht gerundet, aber andere numerische Felder könnten, wenn sie existierten
                rounded_data[key] = rounded_item
            else: # Rekursion in verschachtelte Dictionaries
                rounded_data[key] = _round_numeric_values_for_display(item, decimal_places)
        elif isinstance(item, list): # Rekursion in Listen
            rounded_data[key] = [_round_numeric_values_for_display(elem, decimal_places) for elem in item]
        else: # Nicht-Dictionary-, Nicht-Listen-Elemente werden direkt kopiert
            rounded_data[key] = item
    return rounded_data