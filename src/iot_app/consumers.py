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

            if message_type == 'request_plot_data':
                # Frontend requests plot data for a specific time range
                start_time_str = text_data_json.get('start_time')
                end_time_str = text_data_json.get('end_time')

                start_time = None
                end_time = None

                if start_time_str:
                    # Ensure timezone-aware datetime objects
                    start_time = datetime.fromisoformat(start_time_str)
                    if start_time.tzinfo is None:
                        start_time = timezone.make_aware(start_time)

                # Handle 'live' keyword for end_time
                if end_time_str == 'live':
                    end_time = 'live' # Pass 'live' keyword to send_plot_data
                    self.live_mode_active = True # Activate live mode if end_time is 'live'
                elif end_time_str:
                    # Ensure timezone-aware datetime objects
                    end_time = datetime.fromisoformat(end_time_str)
                    if end_time.tzinfo is None:
                        end_time = timezone.make_aware(end_time)
                    self.live_mode_active = False # Deactivate live mode for fixed end_time
                else:
                    self.live_mode_active = False # Deactivate live mode if no end_time is provided (implies fixed range)

                await self.send_plot_data(start_time, end_time, plot_type='historical_range')
            elif message_type == 'request_initial_data':
                # Frontend requests initial data (e.g., "Aktuellen Lauf anzeigen" button)
                # This should trigger the "last 10 minutes live" behavior
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
        elif message_type == "plot_boundary_update":
            # A new plot boundary has been set, re-send historical data
            # This might be triggered by external events, so re-evaluate live mode
            if self.live_mode_active:
                await self.send_plot_data(plot_type='initial_live_10_min') # Re-send current live 10 min
            else:
                # If not in live mode, just re-send the current fixed range (if any)
                # Or do nothing if it's a fixed range that hasn't changed.
                # For simplicity, we can just re-send the default if no specific range is stored.
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

    # This is the single, combined send_plot_data function
    async def send_plot_data(self, start_time=None, end_time=None, plot_type='initial_historical'):
        """
        Fetches plot data for a given period and sends it to the client.
        If start_time and end_time are not provided, it determines the active run time window
        or defaults to the last 10 minutes.
        If plot_type is 'initial_live_10_min', it forces a 10-minute window ending now.
        """
        current_plot_type = plot_type

        # Handle 'initial_live_10_min' and 'live' end_time
        if current_plot_type == 'initial_live_10_min' or end_time == 'live':
            dynamic_end_time = timezone.now()
            # If start_time is not provided, default to last 10 minutes
            if start_time is None:
                start_time = dynamic_end_time - timedelta(minutes=10)
            end_time = dynamic_end_time # Use the dynamically calculated end_time for fetching data
            self.live_mode_active = True # Ensure live mode is active

        # If no specific time range is provided by the client, and not in initial live mode,
        # use the default logic to get the active run time window or last 10 minutes.
        elif start_time is None and end_time is None:
            start_time, end_time = await self._get_active_run_time_window()
            # If get_active_run_time_window returns (None, None), it means no active run found,
            # so we still want to show the last 10 minutes, but it's a static view unless
            # the user explicitly requests 'live' or initial_live_10_min.
            if start_time is None or end_time is None:
                end_time = timezone.now()
                start_time = end_time - timedelta(minutes=10)
            current_plot_type = 'initial_historical' # This indicates it's the default historical view
            self.live_mode_active = False # Not actively tracking live unless explicitly requested

        # Ensure start_time and end_time are not None before passing to _get_plot_data
        # If _get_active_run_time_window also returns None or partial None, set a default 10-minute window
        if start_time is None or end_time is None:
            end_time = timezone.now()
            start_time = end_time - timedelta(minutes=10)
            current_plot_type = 'initial_historical' # Ensure type is initial if we're defaulting
            self.live_mode_active = False # Not actively tracking live unless explicitly requested

        # Ensure all datetime objects are timezone-aware before passing to _get_plot_data
        # and before serializing to ISO format.
        if start_time and not timezone.is_aware(start_time):
            start_time = timezone.make_aware(start_time)
        if end_time and not timezone.is_aware(end_time):
            end_time = timezone.make_aware(end_time)

        plot_data = await self._get_plot_data(start_time, end_time)

        serializable_plot_data = to_serializable_dict(plot_data)
        await self.send(text_data=json.dumps({
            'type': 'plot_data_update',
            'plot_type': current_plot_type,
            'data': serializable_plot_data,
            'start_time': start_time.isoformat() if start_time else None,
            'end_time': end_time.isoformat() if end_time else None,
            'live_mode_active': self.live_mode_active, # Inform frontend about live mode status
        }))

    async def send_latest_plot_data_point(self):
        """
        Fetches the very latest plot data point and sends it to the client.
        This is used for continuously adding new data to the live plot.
        Only sends if live_mode_active is True.
        """
        if not self.live_mode_active:
            return # Do not send new points if live mode is not active

        latest_data_point = await self._get_latest_plot_data_point()

        # Only send if there is actual new data
        if latest_data_point and (latest_data_point.get('live') or latest_data_point.get('twin') or latest_data_point.get('raw')):
            serializable_data_point = to_serializable_dict(latest_data_point)
            await self.send(text_data=json.dumps({
                'type': 'plot_data_point',
                'data': serializable_data_point,
            }))
        # else:
        #     print("No new plot data point available to send.") # uncomment for debugging