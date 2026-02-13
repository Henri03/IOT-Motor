# path: src/iot_app/consumers.py

# Websocket
# Defines the DashboardConsumer, which is responsible for handling WebSocket connections for a real-time dashboard.
# This script enables bidirectional communication between the frontend (web browser) and the backend (Django application) via WebSockets to display and update data in real time.
# The DashboardConsumer is a central component for providing an an interactive and dynamic dashboard.

import json                                                                                 # Serialization and deserialization of data in JSON format
from channels.generic.websocket import AsyncWebsocketConsumer                               # Basic structure and methods for WebSocket communication
from asgiref.sync import sync_to_async                                                      # To safely execute synchronous functions (like Django ORM calls) in an asynchronous context
from django.utils import timezone
from datetime import datetime, timedelta
import asyncio
import pytz # Import pytz for explicit timezone handling

from .models import MotorInfo, LiveData, TwinData, MalfunctionLog, RawData, FeatureData, PredictionData # Imports Django models that define the IOT application
from .utils import get_dashboard_data, to_serializable_dict                                 # Helper functions that encapsulate the logic for retrieving and processing data from the database
from .utils import get_active_run_time_window, get_plot_data, get_latest_plot_data_point    #

class DashboardConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for real-time dashboard updates.
    Listens to channel layer messages and sends them to clients.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.live_mode_active = False # Flag to control continuous plot updates
        # Stores the start time of the currently displayed plot window.
        # Used to re-request data if a plot_boundary_update occurs in live mode.
        self.current_plot_start_time = None
        # Stores the end time of the currently displayed plot window (if fixed).
        # Not used for live mode where end time is always 'now'.
        self.current_plot_end_time = None

    async def connect(self):
        """
        Handles new WebSocket connections.
        Adds the channel to a group and sends initial data.
        """
        self.group_name = "iot_dashboard_group"                                             # Group name for the dashboard

        await self.channel_layer.group_add(                                                 # Adds the consumer's current channel (the specific WebSocket connection) to the defined group
            self.group_name,
            self.channel_name
        )
        await self.accept()                                                                 # Accepts the incoming WebSocket connection. Without this, the connection would be closed.

        print(f"WebSocket connected: {self.channel_name} to group {self.group_name}")

        await self.send_current_data()                                                      # Sends the current dashboard panel data to the newly connected client.
        # On initial load, request the initial live view (current run or last 10 minutes live)
        await self.send_plot_data(plot_type='initial_load_live')

    async def disconnect(self, close_code):
        """
        Handles WebSocket disconnections.
        Removes the channel from the group and deactivates live mode.
        """
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
                    # live_mode_active will be set in send_plot_data based on logic
                elif end_time_str:
                    end_time = datetime.fromisoformat(end_time_str)
                    if end_time.tzinfo is None:
                        end_time = timezone.make_aware(end_time)
                    self.live_mode_active = False # Deactivate live mode for fixed end_time
                else:
                    # If no end_time_str is provided, it implies a fixed range up to now, so deactivate live mode
                    self.live_mode_active = False

                print(f"Frontend requested plot data: start={start_time_str}, end={end_time_str}. Preparing to call send_plot_data.")
                await self.send_plot_data(start_time=start_time, end_time=end_time, plot_type='historical_or_live_range')

            elif message_type == 'request_initial_data':
                # Frontend requests initial data 
                print("Frontend requested initial data (current run or 10 min live).")
                await self.send_plot_data(plot_type='initial_load_live') # Request initial live view

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
            'message': serializable_data 
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
                # When a new point arrives, we need to ensure the client's X-axis is updated
                # to reflect the sliding window. The client will handle its own x-axis adjustment.
                await self.send_latest_plot_data_point()
            # else:
            #     print("Skipping plot_data_point: Live mode is not active.") # Uncomment for debugging
        elif message_type == "plot_boundary_update":
            # A new plot boundary has been set, re-send historical data
            if self.live_mode_active:
                print("Received plot_boundary_update in live mode, re-requesting initial live view.")
                # Re-send the current live view based on the stored start time or default 10 min
                # If current_plot_start_time is None, send_plot_data will default to 10 min window
                await self.send_plot_data(
                    start_time=self.current_plot_start_time,
                    end_time='live', # Always 'live' for this case
                    plot_type='initial_load_live' # This type ensures the logic for live window is applied
                )
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
        Returns the time window of the active run, or (None, None) if no active run.
        """
        return get_active_run_time_window()

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

        # Case 1: Initial load or "Show current run" button
        if plot_type == 'initial_load_live':
            # Try to get active run, otherwise default to last 10 minutes
            active_run_start, active_run_end = await self._get_active_run_time_window()
            if active_run_start and active_run_end:
                current_start_time = active_run_start
                current_end_time = active_run_end # Will be timezone.now() if run is ongoing
                print(f"send_plot_data: initial_load_live requested. Using active run window: {current_start_time} - {current_end_time}.")
                # If using an active run, the start time is fixed to the run's start
                self.current_plot_start_time = current_start_time
            else:
                # If no active run, default to last 10 minutes from now
                current_end_time = timezone.now()
                current_start_time = current_end_time - timedelta(minutes=10)
                print(f"send_plot_data: initial_load_live requested. No active run, defaulting to last 10 minutes: {current_start_time} - {current_end_time}.")
                # For a 10-minute sliding window, the backend doesn't fix the start time,
                # so we set current_plot_start_time to None to signal the frontend to slide.
                self.current_plot_start_time = None

            is_live_mode_active_for_response = True # Initial load is always live
            self.live_mode_active = True

        # Case 2: Frontend explicitly requested 'live' end_time (manual start time possible)
        elif end_time == 'live':
            current_end_time = timezone.now() # End time is always now in live mode
            if current_start_time is None:
                # If no start_time was provided by the user, default to 10 min window from now
                current_start_time = current_end_time - timedelta(minutes=10)
                print(f"send_plot_data: live end_time requested, no start_time. Setting window to last 10 minutes: {current_start_time} - {current_end_time}. Live mode: TRUE")
                self.current_plot_start_time = None # Frontend will handle sliding
            else:
                # Use the provided start_time with current_end_time as now
                print(f"send_plot_data: live end_time requested with manual start_time. Setting window to {current_start_time} - {current_end_time}. Live mode: TRUE")
                self.current_plot_start_time = current_start_time # Frontend will use this fixed start

            is_live_mode_active_for_response = True
            self.live_mode_active = True

        # Case 3: Fixed historical range (both start_time and end_time are specific datetimes)
        else:
            # If no start/end time provided, determine active run or default to last 10 min static
            # This block should ideally not be reached if initial_load_live is correctly handled,
            # but serves as a fallback for other 'historical_or_live_range' requests without explicit times.
            if current_start_time is None and current_end_time is None:
                active_run_start, active_run_end = await self._get_active_run_time_window()
                if active_run_start and active_run_end:
                    current_start_time = active_run_start
                    current_end_time = active_run_end
                    print(f"send_plot_data: No explicit range, using active run window: {current_start_time} - {current_end_time}. Live mode: FALSE")
                else:
                    # If no active run and no explicit range, default to a static last 10 minutes
                    current_end_time = timezone.now()
                    current_start_time = current_end_time - timedelta(minutes=10)
                    print(f"send_plot_data: No explicit range and no active run, defaulting to static last 10 minutes: {current_start_time} - {current_end_time}. Live mode: FALSE")
            else:
                # This is a manually specified fixed historical range
                print(f"send_plot_data: Fixed historical range requested: {current_start_time} - {current_end_time}. Live mode: FALSE")

            is_live_mode_active_for_response = False
            self.live_mode_active = False
            self.current_plot_start_time = current_start_time # Store fixed start time
            self.current_plot_end_time = current_end_time # Store fixed end time

        # Ensure all datetime objects are timezone-aware
        if current_start_time and not timezone.is_aware(current_start_time):
            current_start_time = timezone.make_aware(current_start_time)
        if current_end_time and current_end_time != 'live' and not timezone.is_aware(current_end_time):
            current_end_time = timezone.make_aware(current_end_time)


        # Fetch data for the determined time window
        # If current_end_time is 'live', pass timezone.now() to get_plot_data
        actual_end_time_for_query = current_end_time if current_end_time != 'live' else timezone.now()
        plot_data = await self._get_plot_data(current_start_time, actual_end_time_for_query)

        serializable_plot_data = to_serializable_dict(plot_data)
        await self.send(text_data=json.dumps({
            'type': 'plot_data_update',
            'plot_type': plot_type, # Keep original plot_type for frontend context
            'data': serializable_plot_data,
            # Send current_plot_start_time which might be None for sliding 10-min window
            'start_time': self.current_plot_start_time.isoformat() if self.current_plot_start_time else None,
            # Send 'live' string if it's live mode, otherwise the fixed end time
            'end_time': current_end_time.isoformat() if current_end_time != 'live' and current_end_time else 'live',
            'live_mode_active': is_live_mode_active_for_response, # Inform frontend about live mode status
        }))
        print(f"Sent plot_data_update: start={self.current_plot_start_time}, end={current_end_time}, live_mode_active={is_live_mode_active_for_response}")

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
