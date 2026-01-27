# IOT_PROJECT/src/iot_app/consumers.py

# Websocket
# Definiert den DashboardConsumer, der für die Handhabung von WebSocket-Verbindungen für ein Echtzeit-Dashboard verantwortlich ist.
# Dieses Skript ermöglicht die bidirektionale Kommunikation zwischen dem Frontend (Webbrowser) und dem Backend (Django-Anwendung) über WebSockets, um Daten in Echtzeit anzuzeigen und zu aktualisieren.
# Der DashboardConsumer ist eine zentrale Komponente für die Bereitstellung eines interaktiven und dynamischen Dashboards.

import json                                                                                 # Serialisierung und Deserialisierung von Daten im JSON-Format
from channels.generic.websocket import AsyncWebsocketConsumer                               # grundlegende Struktur und Methoden für die WebSocket-Kommunikation
from asgiref.sync import sync_to_async                                                      # synchrone Funktionen (wie Django ORM-Aufrufe) sicher in einem asynchronen Kontext auszuführen
from django.utils import timezone
from datetime import datetime, timedelta
import asyncio
import pytz # Import pytz for explicit timezone handling

from .models import MotorInfo, LiveData, TwinData, MalfunctionLog, RawData, FeatureData, PredictionData # Importiert Django-Modelle, die die IOT-Anwendung definieren
from .utils import get_dashboard_data, to_serializable_dict                                 # Hilfsfunktionen, die die Logik zum Abrufen und Verarbeiten von Daten aus der Datenbank kapselt
from .utils import get_active_run_time_window, get_plot_data, get_latest_plot_data_point    #

class DashboardConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for real-time dashboard updates.
    Listens to channel layer messages and sends them to clients.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.live_mode_active = False # Flag to control continuous plot updates

    async def connect(self):

        self.group_name = "iot_dashboard_group"                                             # Group name für das dashboard

        await self.channel_layer.group_add(                                                 # Fügt den aktuellen Kanal des Consumers (die spezifische WebSocket-Verbindung) zur definierten Gruppe hinzu
            self.group_name,
            self.channel_name
        )
        await self.accept()                                                                 #  Akzeptiert die eingehende WebSocket-Verbindung. Ohne dies würde die Verbindung geschlossen.

        print(f"WebSocket connected: {self.channel_name} to group {self.group_name}")

        await self.send_current_data()                                                      # Sendet die aktuellen Dashboard-Panel-Daten an den neu verbundenen Client.
        # On initial load, set live_mode_active to True and request last 10 minutes
        self.live_mode_active = True
        await self.send_plot_data(plot_type='initial_live_10_min') # Calls send_plot_data with a specific type for initial live load

    async def disconnect(self, close_code):
        print(f"WebSocket disconnected: {self.channel_name}")

        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )
        self.live_mode_active = False # Ensure live mode is deactivated on disconnect

    async def receive(self, text_data):
        """
        Receives messages from the WebSocket (frontend) for plot control.
        """
        if text_data:
            text_data_json = json.loads(text_data)
            message_type = text_data_json.get('type')
            print(f"Received message from frontend: {message_type}")

            if message_type == 'request_plot_data':
                # Frontend requests plot data for a specific time range
                start_time_str = text_data_json.get('start_time')
                end_time_str = text_data_json.get('end_time')

                start_time = None
                end_time = None

                if start_time_str:
                    start_time = datetime.fromisoformat(start_time_str)
                    if start_time.tzinfo is None:
                        start_time = timezone.make_aware(start_time)

                if end_time_str == 'live':
                    end_time = 'live' # Keep as string 'live' for send_plot_data to handle
                    self.live_mode_active = True # Activate live mode
                elif end_time_str:
                    end_time = datetime.fromisoformat(end_time_str)
                    if end_time.tzinfo is None:
                        end_time = timezone.make_aware(end_time)
                    self.live_mode_active = False # Deactivate live mode for fixed end_time
                else:
                    # If no end_time_str is provided, it implies a fixed range up to now, so deactivate live mode
                    self.live_mode_active = False

                print(f"Frontend requested plot data: start={start_time_str}, end={end_time_str}, live_mode_active={self.live_mode_active}")
                await self.send_plot_data(start_time=start_time, end_time=end_time, plot_type='historical_range')

            elif message_type == 'request_initial_data':
                # Frontend requests initial data (e.g., "Aktuellen Lauf anzeigen" button or initial load)
                print("Frontend requested initial data (live 10 min).")
                self.live_mode_active = True
                await self.send_plot_data(plot_type='initial_live_10_min') # Request initial live 10 min

            else:
                print(f"Unknown message type received from frontend: {message_type}")

    async def send_current_data(self):
        """
        Fetches the latest motor and twin data and sends it to the client.
        """
        data = await self._get_latest_motor_data()
        # Convert any remaining datetime objects to ISO strings before JSON serialization
        serializable_data = to_serializable_dict(data)
        await self.send(text_data=json.dumps({
            'type': 'dashboard_update',
            'message': serializable_data # Use the serializable data
        }))

    async def dashboard_message(self, event):
        """
        Receives a channel layer message and sends it to the WebSocket.
        Handles different message types for dashboard updates and plot data.
        """
        message_type = event.get('message_type')
        data = event.get('data')
        legacy_message = event.get('message')

        if message_type == "plot_data_point":
            # Send only the latest data point for continuous plots IF live mode is active
            if self.live_mode_active:
                await self.send_latest_plot_data_point()
            else:
                # print("Skipping plot_data_point: Live mode is not active.") # Uncomment for debugging
                pass
        elif message_type == "plot_boundary_update":
            # A new plot boundary has been set, re-send historical data
            # This might be triggered by external events, so re-evaluate live mode
            if self.live_mode_active:
                print("Received plot_boundary_update in live mode, re-requesting initial live 10 min.")
                await self.send_plot_data(plot_type='initial_live_10_min') # Re-send current live 10 min
            else:
                print("Received plot_boundary_update, but not in live mode. No action taken.")
                pass # The frontend will handle re-requesting if needed for fixed ranges
        elif message_type == "dashboard_update":
            # General dashboard update (panel data, anomaly status)
            serializable_data = to_serializable_dict(data)
            await self.send(text_data=json.dumps({
                'type': 'dashboard_update',
                'message': serializable_data
            }))
        elif legacy_message:
            print(f"WARN: Received legacy dashboard message. Please update MQTT consumer to use 'message_type' and 'data'.")
            serializable_legacy_message = to_serializable_dict(legacy_message)
            await self.send(text_data=json.dumps({
                'type': 'dashboard_update',
                'message': serializable_legacy_message
            }))
        else:
            print(f"Unhandled dashboard_message type from channel layer: {message_type}. Full event: {event}")

    @sync_to_async
    def _get_latest_motor_data(self):
        """Synchronously retrieves the latest motor data for the dashboard."""
        return get_dashboard_data()

    @sync_to_async
    def _get_active_run_time_window(self):
        """
        Wrapper for get_active_run_time_window utility function.
        Returns the time window of the active run, or the last 10 minutes if no active run.
        """
        start_time, end_time = get_active_run_time_window()
        if not start_time and not end_time:
            # If no active run is detected, default to the last 10 minutes
            end_time = timezone.now()
            start_time = end_time - timedelta(minutes=10)
        return start_time, end_time

    @sync_to_async
    def _get_plot_data(self, start_time, end_time):
        """Wrapper for get_plot_data utility function."""
        return get_plot_data(start_time, end_time)

    @sync_to_async
    def _get_latest_plot_data_point(self):
        """Wrapper for get_latest_plot_data_point utility function."""
        return get_latest_plot_data_point()

    async def send_plot_data(self, start_time=None, end_time=None, plot_type='initial_historical'):
        """
        Fetches plot data for a given period and sends it to the client.
        This function now explicitly handles the logic for setting the time window
        and the live_mode_active flag based on the request.
        """
        current_start_time = start_time
        current_end_time = end_time
        is_live_mode_active_for_response = False

        # Case 1: Initial load or "Show current run" button -> always 10 min live
        if plot_type == 'initial_live_10_min':
            current_end_time = timezone.now()
            current_start_time = current_end_time - timedelta(minutes=10)
            is_live_mode_active_for_response = True
            self.live_mode_active = True
            print(f"send_plot_data: initial_live_10_min requested. Setting window to {current_start_time} - {current_end_time}. Live mode: TRUE")

        # Case 2: Frontend explicitly requested 'live' end_time
        elif end_time == 'live':
            current_end_time = timezone.now()
            # If a start_time was provided, use it, otherwise default to 10 min window
            if current_start_time is None:
                current_start_time = current_end_time - timedelta(minutes=10)
            is_live_mode_active_for_response = True
            self.live_mode_active = True
            print(f"send_plot_data: live end_time requested. Setting window to {current_start_time} - {current_end_time}. Live mode: TRUE")

        # Case 3: Fixed historical range or default behavior (active run / last 10 min static)
        else:
            # If no start/end time provided, determine active run or default to last 10 min static
            if current_start_time is None and current_end_time is None:
                current_start_time, current_end_time = await self._get_active_run_time_window()
                if current_start_time is None or current_end_time is None:
                    current_end_time = timezone.now()
                    current_start_time = current_end_time - timedelta(minutes=10)
                print(f"send_plot_data: No explicit range, using active run/default 10 min static: {current_start_time} - {current_end_time}. Live mode: FALSE")
            else:
                print(f"send_plot_data: Fixed historical range requested: {current_start_time} - {current_end_time}. Live mode: FALSE")

            is_live_mode_active_for_response = False
            self.live_mode_active = False

        # Ensure all datetime objects are timezone-aware
        if current_start_time and not timezone.is_aware(current_start_time):
            current_start_time = timezone.make_aware(current_start_time)
        if current_end_time and not timezone.is_aware(current_end_time):
            current_end_time = timezone.make_aware(current_end_time)

        plot_data = await self._get_plot_data(current_start_time, current_end_time)

        serializable_plot_data = to_serializable_dict(plot_data)
        await self.send(text_data=json.dumps({
            'type': 'plot_data_update',
            'plot_type': plot_type, # Keep original plot_type for frontend context
            'data': serializable_plot_data,
            'start_time': current_start_time.isoformat() if current_start_time else None,
            'end_time': current_end_time.isoformat() if current_end_time else None,
            'live_mode_active': is_live_mode_active_for_response, # Inform frontend about live mode status
        }))
        print(f"Sent plot_data_update: start={current_start_time}, end={current_end_time}, live_mode_active={is_live_mode_active_for_response}")

    async def send_latest_plot_data_point(self):
        """
        Fetches the very latest LiveData, TwinData, RawData, FeatureData, and PredictionData points
        and sends them to the client. This is used for continuous live updates.
        Only sends if live_mode_active is True.
        """
        if not self.live_mode_active:
            return # Do not send new points if live mode is not active

        latest_data_point = await self._get_latest_plot_data_point()

        # Only send if there is actual new data for any of the categories
        if latest_data_point and (latest_data_point.get('live') or
                                  latest_data_point.get('twin') or
                                  latest_data_point.get('raw') or
                                  latest_data_point.get('feature') or
                                  latest_data_point.get('prediction')):
            serializable_data_point = to_serializable_dict(latest_data_point)
            await self.send(text_data=json.dumps({
                'type': 'plot_data_point',
                'data': serializable_data_point,
            }))
            # print(f"Sent latest plot data point: {serializable_data_point.get('raw', {}).get('current', 'N/A')}") # Uncomment for debugging
        # else:
        #     print("No new plot data point available to send.") # uncomment for debugging