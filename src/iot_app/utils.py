# IOT_PROJECT/src/iot_app/utils.py
from .models import LiveData, TwinData, MalfunctionLog, MotorInfo, ReferenceRun
from django.utils import timezone
from datetime import timedelta, datetime
from django.db.models import Q

def get_latest_live_data():
    """
    Retrieves the latest live data and returns it in a standardized format.
    Returns all expected keys, even if no data is available.
    """
    data = {
        'Strom': {'value': None, 'unit': 'A'},
        'Spannung': {'value': None, 'unit': 'V'},
        'Drehzahl': {'value': None, 'unit': 'U/min'},
        'Vibration': {'value': None, 'unit': 'mm/s'},
        'Temperatur': {'value': None, 'unit': '°C'},
        'Drehmoment': {'value': None, 'unit': 'Nm'},
        'Laufzeit': {'value': None, 'unit': 'h'},
    }

    latest = LiveData.objects.order_by('-timestamp').first()
    if latest:
        data['Strom']['value'] = latest.current
        data['Spannung']['value'] = latest.voltage
        data['Drehzahl']['value'] = latest.rpm
        data['Vibration']['value'] = latest.vibration
        data['Temperatur']['value'] = latest.temp
        if hasattr(latest, 'torque'):
            data['Drehmoment']['value'] = latest.torque
        if hasattr(latest, 'run_time'):
            data['Laufzeit']['value'] = latest.run_time
    return data

def get_latest_digital_twin():
    """
    Retrieves the latest Digital Twin (ReferenceRun) data and returns it in a standardized format.
    Returns all expected keys, even if no data is available.
    """
    # Note: ReferenceRun is used here to display the CURRENT twin data in the panel,
    # while TwinData is used for historical plots.
    data = {
        'Strom': {'value': None, 'unit': 'A'},
        'Spannung': {'value': None, 'unit': 'V'},
        'Drehzahl': {'value': None, 'unit': 'U/min'},
        'Vibration': {'value': None, 'unit': 'mm/s'},
        'Temperatur': {'value': None, 'unit': '°C'},
        'Drehmoment': {'value': None, 'unit': 'Nm'},
        'Laufzeit': {'value': None, 'unit': 'h'},
    }

    # For displaying the *current* twin values in the panel, we use the last valid ReferenceRun.
    ref_data = ReferenceRun.objects.filter(is_valid=True).order_by('-timestamp').first()
    if ref_data:
        data['Strom']['value'] = ref_data.current
        data['Spannung']['value'] = ref_data.voltage
        data['Drehzahl']['value'] = ref_data.rpm
        data['Vibration']['value'] = ref_data.vibration
        data['Temperatur']['value'] = ref_data.temp
        if hasattr(ref_data, 'torque'):
            data['Drehmoment']['value'] = ref_data.torque
        if hasattr(ref_data, 'run_time'):
            data['Laufzeit']['value'] = ref_data.run_time
    return data

def get_anomaly_status():
    """
    Determines the current anomaly status based on live and twin data, as well as error messages.
    This function is not directly called by get_dashboard_data in the current setup;
    anomaly detection happens in the MQTT consumer.
    It is kept here in case it is needed elsewhere.
    """
    latest_live = LiveData.objects.order_by('-timestamp').first()
    # For anomaly detection, ReferenceRun is still used here as the source for "expected values" in the UI.
    latest_twin_for_comparison = ReferenceRun.objects.filter(is_valid=True).order_by('-timestamp').first()
    latest_malfunction_log = MalfunctionLog.objects.order_by('-timestamp').first()

    anomaly_status = {"detected": False, "message": "No anomaly detected."}

    if latest_malfunction_log:
        if latest_malfunction_log.message_type in ['ERROR', 'WARNING']:
            anomaly_status["detected"] = True
            anomaly_status["message"] = f"CRITICAL ERROR: Motor fault reported! ({latest_malfunction_log.description})"
            return anomaly_status
        elif latest_malfunction_log.message_type == 'INFO':
            anomaly_status["detected"] = False
            anomaly_status["message"] = f"Information: {latest_malfunction_log.description}"

    if not latest_live or not latest_twin_for_comparison:
        if not anomaly_status["detected"]:
            anomaly_status["detected"] = False
            anomaly_status["message"] = "No data available."
        return anomaly_status

    if not anomaly_status["detected"]:
        # Example: Current deviation
        if latest_twin_for_comparison.current is not None and latest_live.current is not None:
            if abs(latest_live.current - latest_twin_for_comparison.current) > 0.2 * latest_twin_for_comparison.current:
                anomaly_status["detected"] = True
                anomaly_status["message"] = "Current out of expected range!"
                return anomaly_status

        # Example: RPM deviation
        if latest_twin_for_comparison.rpm is not None and latest_live.rpm is not None:
            if abs(latest_live.rpm - latest_twin_for_comparison.rpm) > 0.2 * latest_twin_for_comparison.rpm:
                anomaly_status["detected"] = True
                anomaly_status["message"] = "RPM out of expected range!"
                return anomaly_status

        # Example: Voltage deviation
        if latest_twin_for_comparison.voltage is not None and latest_live.voltage is not None:
            if abs(latest_live.voltage - latest_twin_for_comparison.voltage) > 0.2 * latest_twin_for_comparison.voltage:
                anomaly_status["detected"] = True
                anomaly_status["message"] = "Voltage out of expected range!"
                return anomaly_status

        # Vibration and temperature added for anomaly detection
        if latest_twin_for_comparison.vibration is not None and latest_live.vibration is not None:
            if abs(latest_live.vibration - latest_twin_for_comparison.vibration) > 0.2 * latest_twin_for_comparison.vibration:
                anomaly_status["detected"] = True
                anomaly_status["message"] = "Vibration out of expected range!"
                return anomaly_status

        if latest_twin_for_comparison.temp is not None and latest_live.temp is not None:
            if abs(latest_live.temp - latest_twin_for_comparison.temp) > 0.2 * latest_twin_for_comparison.temp:
                anomaly_status["detected"] = True
                anomaly_status["message"] = "Temperature out of expected range!"
                return anomaly_status

        if latest_twin_for_comparison.torque is not None and latest_live.torque is not None:
            if abs(latest_live.torque - latest_twin_for_comparison.torque) > 0.2 * latest_twin_for_comparison.torque:
                anomaly_status["detected"] = True
                anomaly_status["message"] = "Torque out of expected range!"
                return anomaly_status

        if latest_twin_for_comparison.run_time is not None and latest_live.run_time is not None:
            if abs(latest_live.run_time - latest_twin_for_comparison.run_time) > 0.2 * latest_twin_for_comparison.run_time:
                anomaly_status["detected"] = True
                anomaly_status["message"] = "Run time out of expected range!"
                return anomaly_status

    return anomaly_status

def get_dashboard_data():
    """
    Combines all necessary information into a complete dataset for the dashboard.
    This function is only used for initial data when loading the page.
    Current data and anomaly status are received via WebSockets.
    """
    motor_info_obj = MotorInfo.objects.first()
    motor_info_data = {
        'name': 'Unknown Motor',
        'model': 'N/A',
        'description': 'No motor information found.',
        'identification': 'N/A',
        'location': 'N/A',
        'commissioning_date': None,
        'cycles': 0,
        'operating_mode': 'N/A'
    }
    if motor_info_obj:
        motor_info_data = {
            "name": motor_info_obj.name,
            "model": motor_info_obj.model,
            "description": motor_info_obj.description,
            "identification": motor_info_obj.identification,
            "location": motor_info_obj.location,
            "commissioning_date": motor_info_obj.commissioning_date.isoformat() if motor_info_obj.commissioning_date else None,
            "cycles": motor_info_obj.cycles,
            "operating_mode": motor_info_obj.operating_mode,
        }

    real_motor_data = get_latest_live_data()
    digital_twin_data = get_latest_digital_twin()
    # Anomaly status is no longer calculated here, but passed from the MQTT consumer
    # and processed in the consumer as part of the 'dashboard_update'.
    # For the initial call when loading the page, a default status could be returned here,
    # or the frontend waits for the first WebSocket update.

    # Retrieve the last 5 malfunction logs (if needed)
    latest_malfunction_logs = MalfunctionLog.objects.order_by('-timestamp')[:5]
    malfunction_logs_serializable = []
    for log in latest_malfunction_logs:
        malfunction_logs_serializable.append({
            'id': log.id,
            'timestamp': log.timestamp.isoformat(),
            'message_type': log.message_type,
            'description': log.description,
            'motor_state': log.motor_state,
            'emergency_stop_active': log.emergency_stop_active,
        })

    return {
        'motor_info': motor_info_data,
        'real_motor_data': real_motor_data,
        'digital_twin_data': digital_twin_data,
        'anomaly_status': {"detected": False, "message": "Waiting for status update..."}, # Initial placeholder
        'malfunction_logs': malfunction_logs_serializable,
    }

# FUNCTIONS FOR PLOTS
def get_active_run_time_window():
    """
    Determines the time window of the last active motor run based on MalfunctionLog entries.
    Looks for the last "motor starts" or "motor stops" and the subsequent "end position reached".
    Returns (start_time, end_time). end_time is None if the motor is still running.
    """
    # Find the last start event (motor starts/stops)
    latest_start_event = MalfunctionLog.objects.filter(
        Q(description__icontains='motor fährt ein') | Q(description__icontains='motor fährt aus'),
        message_type='INFO'
    ).order_by('-timestamp').first()

    start_time = None
    end_time = None

    if latest_start_event:
        start_time = latest_start_event.timestamp

        # Find the first stop event ("end position reached") AFTER the last start event
        latest_stop_event_after_start = MalfunctionLog.objects.filter(
            Q(description__icontains='Endlage erreicht'),
            message_type='INFO',
            timestamp__gte=start_time
        ).order_by('timestamp').first() # The first stop after the start

        if latest_stop_event_after_start:
            # The motor reached an end position after the last start.
            end_time = latest_stop_event_after_start.timestamp
        else:
            # The motor started, but has not yet reached an end position.
            # So it is still running, end_time is None (for now).
            end_time = None

    return start_time, end_time

def get_plot_data(start_time=None, end_time=None):
    """
    Retrieves LiveData and TwinData for a specific period and formats them for Chart.js.
    :param start_time: datetime object, start time for the data.
    :param end_time: datetime object, end time for the data. If None, up to now.
    :return: Dictionary with formatted data for Live and Twin.
    """
    live_data_queryset = LiveData.objects.all()
    twin_data_queryset = TwinData.objects.all() # TwinData for plots

    if start_time:
        live_data_queryset = live_data_queryset.filter(timestamp__gte=start_time)
        twin_data_queryset = twin_data_queryset.filter(timestamp__gte=start_time)

    # If no end_time is given, we fetch data up to the current time
    if end_time:
        live_data_queryset = live_data_queryset.filter(timestamp__lte=end_time)
        twin_data_queryset = twin_data_queryset.filter(timestamp__lte=end_time)
    else:
        # If end_time is None, we fetch data up to now (for live plotting)
        # Filtering for <= timezone.now() is implicit as auto_now_add is used.
        # It is not necessary to explicitly filter here, as the data only exists up to now anyway.
        pass

    live_data_queryset = live_data_queryset.order_by('timestamp')
    twin_data_queryset = twin_data_queryset.order_by('timestamp')

    # Formatting data for Chart.js
    # Each Chart.js dataset requires an array of {x: timestamp, y: value} objects.
    plot_data = {
        'live': {
            'current': [], 'voltage': [], 'rpm': [], 'vibration': [], 'temp': []
        },
        'twin': { # Twin data for plots
            'current': [], 'voltage': [], 'rpm': [], 'vibration': [], 'temp': []
        }
    }

    for entry in live_data_queryset:
        ts = entry.timestamp.isoformat() # ISO format is good for JavaScript Date objects
        if entry.current is not None: plot_data['live']['current'].append({'x': ts, 'y': entry.current})
        if entry.voltage is not None: plot_data['live']['voltage'].append({'x': ts, 'y': entry.voltage})
        if entry.rpm is not None: plot_data['live']['rpm'].append({'x': ts, 'y': entry.rpm})
        if entry.vibration is not None: plot_data['live']['vibration'].append({'x': ts, 'y': entry.vibration})
        if entry.temp is not None: plot_data['live']['temp'].append({'x': ts, 'y': entry.temp})

    for entry in twin_data_queryset: # Iterate twin data
        ts = entry.timestamp.isoformat()
        if entry.current is not None: plot_data['twin']['current'].append({'x': ts, 'y': entry.current})
        if entry.voltage is not None: plot_data['twin']['voltage'].append({'x': ts, 'y': entry.voltage})
        if entry.rpm is not None: plot_data['twin']['rpm'].append({'x': ts, 'y': entry.rpm})
        if entry.vibration is not None: plot_data['twin']['vibration'].append({'x': ts, 'y': entry.vibration})
        if entry.temp is not None: plot_data['twin']['temp'].append({'x': ts, 'y': entry.temp})

    return plot_data

def get_latest_plot_data_point():
    """
    Retrieves the very latest LiveData and TwinData point and formats it.
    Useful for continuous live updates.
    """
    latest_live = LiveData.objects.order_by('-timestamp').first()
    latest_twin = TwinData.objects.order_by('-timestamp').first()

    data_point = {
        'live': {},
        'twin': {}
    }

    if latest_live:
        ts = latest_live.timestamp.isoformat()
        if latest_live.current is not None: data_point['live']['current'] = {'x': ts, 'y': latest_live.current}
        if latest_live.voltage is not None: data_point['live']['voltage'] = {'x': ts, 'y': latest_live.voltage}
        if latest_live.rpm is not None: data_point['live']['rpm'] = {'x': ts, 'y': latest_live.rpm}
        if latest_live.vibration is not None: data_point['live']['vibration'] = {'x': ts, 'y': latest_live.vibration}
        if latest_live.temp is not None: data_point['live']['temp'] = {'x': ts, 'y': latest_live.temp}

    if latest_twin: # Twin data
        ts = latest_twin.timestamp.isoformat()
        if latest_twin.current is not None: data_point['twin']['current'] = {'x': ts, 'y': latest_twin.current}
        if latest_twin.voltage is not None: data_point['twin']['voltage'] = {'x': ts, 'y': latest_twin.voltage}
        if latest_twin.rpm is not None: data_point['twin']['rpm'] = {'x': ts, 'y': latest_twin.rpm}
        if latest_twin.vibration is not None: data_point['twin']['vibration'] = {'x': ts, 'y': latest_twin.vibration}
        if latest_twin.temp is not None: data_point['twin']['temp'] = {'x': ts, 'y': latest_twin.temp}

    return data_point