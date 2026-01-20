# IOT_PROJECT/src/iot_app/consumers.py

# Websocket
# Definiert den DashboardConsumer, der für die Handhabung von WebSocket-Verbindungen für ein Echtzeit-Dashboard verantwortlich ist. 
# Dieses Skript ermöglicht die bidirektionale Kommunikation zwischen dem Frontend (Webbrowser) und dem Backend (Django-Anwendung) über WebSockets, um Daten in Echtzeit anzuzeigen und zu aktualisieren.
# Der DashboardConsumer ist eine zentrale Komponente für die Bereitstellung eines interaktiven und dynamischen Dashboards.

import json                                                                                 # Serialisierung und Deserialisierung von Daten im JSON-Format
from channels.generic.websocket import AsyncWebsocketConsumer                               # grundlegende Struktur und Methoden für die WebSocket-Kommunikation
from asgiref.sync import sync_to_async                                                      # synchrone Funktionen (wie Django ORM-Aufrufe) sicher in einem asynchronen Kontext auszuführen
from django.utils import timezone                                                           
from datetime import datetime

from .models import MotorInfo, LiveData, TwinData, MalfunctionLog                           # Importiert Django-Modelle, die die IOT-Anwendung definieren
from .utils import get_dashboard_data                                                       # Hilfsfunktionen, die die Logik zum Abrufen und Verarbeiten von Daten aus der Datenbank kapseln
from .utils import get_active_run_time_window, get_plot_data, get_latest_plot_data_point    # 

class DashboardConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for real-time dashboard updates.
    Listens to channel layer messages and sends them to clients.
    """

    async def connect(self):
        
        self.group_name = "iot_dashboard_group"                                             # Group name für das dashboard

        await self.channel_layer.group_add(                                                 # Fügt den aktuellen Kanal des Consumers (die spezifische WebSocket-Verbindung) zur definierten Gruppe hinzu
            self.group_name,
            self.channel_name
        )
        await self.accept()                                                                 #  Akzeptiert die eingehende WebSocket-Verbindung. Ohne dies würde die Verbindung geschlossen.

        print(f"WebSocket connected: {self.channel_name} to group {self.group_name}")

        await self.send_current_data()                                                      # Sendet die aktuellen Dashboard-Panel-Daten an den neu verbundenen Client.
        await self.send_plot_data()                                                         # 

    async def disconnect(self, close_code):
        print(f"WebSocket disconnected: {self.channel_name}")

        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )

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

                start_time = datetime.fromisoformat(start_time_str) if start_time_str else None
                end_time = datetime.fromisoformat(end_time_str) if end_time_str else None

                # Ensure timestamps are timezone-aware
                if start_time and start_time.tzinfo is None:
                    start_time = timezone.make_aware(start_time)
                if end_time and end_time.tzinfo is None:
                    end_time = timezone.make_aware(end_time)

                await self.send_plot_data(start_time, end_time, plot_type='historical_range')
            elif message_type == 'request_initial_data':
                # Frontend requests initial data 
                await self.send_plot_data()
            else:
                print(f"Unknown message type received from frontend: {message_type}")

    async def send_current_data(self):
        """
        Fetches the latest motor and twin data and sends it to the client.
        """
        data = await self._get_latest_motor_data()
        await self.send(text_data=json.dumps({
            'type': 'dashboard_update',
            'message': data
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
            # Send only the latest data point for continuous plots
            await self.send_latest_plot_data_point()
        elif message_type == "plot_boundary_update":
            # A new plot boundary has been set, re-send historical data
            await self.send_plot_data()
        elif message_type == "dashboard_update":
            # General dashboard update (panel data, anomaly status)
            await self.send(text_data=json.dumps({
                'type': 'dashboard_update',
                'message': data
            }))
        elif legacy_message:
            print(f"WARN: Received legacy dashboard message. Please update MQTT consumer to use 'message_type' and 'data'.")
            await self.send(text_data=json.dumps({
                'type': 'dashboard_update',
                'message': legacy_message
            }))
        else:
            print(f"Unhandled dashboard_message type from channel layer: {message_type}. Full event: {event}")

    @sync_to_async
    def _get_latest_motor_data(self):
        """Synchronously retrieves the latest motor data for the dashboard."""
        return get_dashboard_data()

    @sync_to_async
    def _get_active_run_time_window(self):
        """Wrapper for get_active_run_time_window utility function."""
        return get_active_run_time_window()

    @sync_to_async
    def _get_plot_data(self, start_time, end_time):
        """Wrapper for get_plot_data utility function."""
        return get_plot_data(start_time, end_time)

    @sync_to_async
    def _get_latest_plot_data_point(self):
        """Wrapper for get_latest_plot_data_point utility function."""
        return get_latest_plot_data_point()

    async def send_plot_data(self):
        """
        Determines the current plot time window and sends the corresponding historical data.
        """
        start_time, end_time = await self._get_active_run_time_window()
        await self.send_plot_data(start_time, end_time, plot_type='initial_historical')

    async def send_plot_data(self, start_time, end_time, plot_type='historical_range'):
        """
        Fetches plot data for a given period and sends it to the client.
        """
        plot_data = await self._get_plot_data(start_time, end_time)
        await self.send(text_data=json.dumps({
            'type': 'plot_data_update',
            'plot_type': plot_type,
            'data': plot_data,
            'start_time': start_time.isoformat() if start_time else None,
            'end_time': end_time.isoformat() if end_time else None,
        }))

    async def send_latest_plot_data_point(self):
        """
        Sends the very latest data point for live plot updates.
        """
        latest_data_point = await self._get_latest_plot_data_point()
        await self.send(text_data=json.dumps({
            'type': 'plot_data_point',
            'data': latest_data_point
        }))