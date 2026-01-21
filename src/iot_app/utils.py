# IOT_PROJECT/src/iot_app/utils.py
from .models import LiveData, TwinData, MalfunctionLog, MotorInfo, ReferenceRun, RawData, FeatureData, PredictionData
from django.utils import timezone
from datetime import timedelta, datetime
from django.db.models import Q

# iot_app/utils.py acts as a toolbox for your IoT application, providing reusable functions to fetch, process, and format data, especially for presentation on the dashboard.

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
    
    data = {
        'Strom': {'value': None, 'unit': 'A'},
        'Spannung': {'value': None, 'unit': 'V'},
        'Drehzahl': {'value': None, 'unit': 'U/min'},
        'Vibration': {'value': None, 'unit': 'mm/s'},
        'Temperatur': {'value': None, 'unit': '°C'},
        'Drehmoment': {'value': None, 'unit': 'Nm'},
        'Laufzeit': {'value': None, 'unit': 'h'},
    }

    # For displaying the *current* twin values in the panel use the last valid ReferenceRun.
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

def get_latest_raw_data_for_dashboard():
    """
    Retrieves the latest raw data for temperature, current, and torque.
    Returns a dictionary formatted for the dashboard.
    """
    latest_raw_data = {
        'temperature': RawData.objects.filter(metric_type='temperature').order_by('-timestamp').first(),
        'current': RawData.objects.filter(metric_type='current').order_by('-timestamp').first(),
        'torque': RawData.objects.filter(metric_type='torque').order_by('-timestamp').first(),
    }
    dashboard_raw_data = {
        metric: {'value': getattr(data, 'value', None), 'timestamp': getattr(data, 'timestamp', None)}
        for metric, data in latest_raw_data.items()
    }
    return dashboard_raw_data

def get_latest_feature_data_for_dashboard():
    """
    Retrieves the latest feature data for temperature, current, and torque.
    Returns a dictionary formatted for the dashboard.
    """
    latest_feature_data = {
        'temperature': FeatureData.objects.filter(metric_type='temperature').order_by('-timestamp').first(),
        'current': FeatureData.objects.filter(metric_type='current').order_by('-timestamp').first(),
        'torque': FeatureData.objects.filter(metric_type='torque').order_by('-timestamp').first(),
    }
    dashboard_feature_data = {
        metric: {
            'mean': getattr(data, 'mean', None),
            'min': getattr(data, 'min_val', None),
            'max': getattr(data, 'max_val', None),
            'median': getattr(data, 'median', None),
            'std': getattr(data, 'std_dev', None),
            'range': getattr(data, 'data_range', None),
            'timestamp': getattr(data, 'timestamp', None)
        } for metric, data in latest_feature_data.items()
    }
    return dashboard_feature_data

def get_latest_prediction_data_for_dashboard():
    """
    Retrieves the latest prediction data for temperature, current, and torque.
    Returns a dictionary formatted for the dashboard.
    """
    latest_prediction_data = {
        'temperature': PredictionData.objects.filter(metric_type='temperature').order_by('-timestamp').first(),
        'current': PredictionData.objects.filter(metric_type='current').order_by('-timestamp').first(),
        'torque': PredictionData.objects.filter(metric_type='torque').order_by('-timestamp').first(),
    }
    dashboard_prediction_data = {
        metric: {
            'predicted_value': getattr(data, 'predicted_value', None),
            'anomaly_score': getattr(data, 'anomaly_score', None),
            'rul_hours': getattr(data, 'rul_hours', None),
            'timestamp': getattr(data, 'timestamp', None)
        } for metric, data in latest_prediction_data.items()
    }
    return dashboard_prediction_data

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

    dashboard_raw_data = get_latest_raw_data_for_dashboard()
    dashboard_feature_data = get_latest_feature_data_for_dashboard()
    dashboard_prediction_data = get_latest_prediction_data_for_dashboard()

    # For the initial call when loading the page, a default status could be returned here,
    # or the frontend waits for the first WebSocket update.

    # Retrieve the last 5 malfunction logs (if needed)
    latest_malfunction_logs = MalfunctionLog.objects.order_by('-timestamp')[:5]
    malfunction_logs_serializable = []
    for log in latest_malfunction_logs:
        malfunction_logs_serializable.append({
            'id': log.id,
            'timestamp': log.timestamp.isoformat() if log.timestamp else None,
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
        'raw_data': dashboard_raw_data,
        'feature_data': dashboard_feature_data,
        'prediction_data': dashboard_prediction_data,
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
    raw_data_queryset = RawData.objects.all()
    feature_data_queryset = FeatureData.objects.all()
    prediction_data_queryset = PredictionData.objects.all()

    if start_time:
        live_data_queryset = live_data_queryset.filter(timestamp__gte=start_time)
        twin_data_queryset = twin_data_queryset.filter(timestamp__gte=start_time)
        raw_data_queryset = raw_data_queryset.filter(timestamp__gte=start_time)
        feature_data_queryset = feature_data_queryset.filter(timestamp__gte=start_time)
        prediction_data_queryset = prediction_data_queryset.filter(timestamp__gte=start_time)

    # If no end_time is given, we fetch data up to the current time
    if end_time:
        live_data_queryset = live_data_queryset.filter(timestamp__lte=end_time)
        twin_data_queryset = twin_data_queryset.filter(timestamp__lte=end_time)
        raw_data_queryset = raw_data_queryset.filter(timestamp__lte=end_time)
        feature_data_queryset = feature_data_queryset.filter(timestamp__lte=end_time)
        prediction_data_queryset = prediction_data_queryset.filter(timestamp__lte=end_time)

    

    live_data_queryset = live_data_queryset.order_by('timestamp')
    twin_data_queryset = twin_data_queryset.order_by('timestamp')
    raw_data_queryset = raw_data_queryset.order_by('timestamp')
    feature_data_queryset = feature_data_queryset.order_by('timestamp')
    prediction_data_queryset = prediction_data_queryset.order_by('timestamp')

    # Formatting data for Chart.js
    # Each Chart.js dataset requires an array of {x: timestamp, y: value} objects.
    plot_data = {
        'live': {
            'current': [], 'voltage': [], 'rpm': [], 'vibration': [], 'temp': []
        },
        'twin': { # Twin data for plots
            'current': [], 'voltage': [], 'rpm': [], 'vibration': [], 'temp': []
        }, 'raw': {
            'temperature': [], 'current': [], 'torque': []
        },
        'feature': {
            'temperature_mean': [], 'temperature_min': [], 'temperature_max': [], 'temperature_median': [], 'temperature_std': [], 'temperature_range': [],
            'current_mean': [], 'current_min': [], 'current_max': [], 'current_median': [], 'current_std': [], 'current_range': [],
            'torque_mean': [], 'torque_min': [], 'torque_max': [], 'torque_median': [], 'torque_std': [], 'torque_range': [],
        },
        'prediction': {
            'temperature_predicted_value': [], 'temperature_anomaly_score': [], 'temperature_rul_hours': [],
            'current_predicted_value': [], 'current_anomaly_score': [], 'current_rul_hours': [],
            'torque_predicted_value': [], 'torque_anomaly_score': [], 'torque_rul_hours': [],
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
 
    # Process RawData
    for entry in raw_data_queryset:
        ts = entry.timestamp.isoformat()
        if entry.metric_type == 'temperature' and entry.value is not None: plot_data['raw']['temperature'].append({'x': ts, 'y': entry.value})
        if entry.metric_type == 'current' and entry.value is not None: plot_data['raw']['current'].append({'x': ts, 'y': entry.value})
        if entry.metric_type == 'torque' and entry.value is not None: plot_data['raw']['torque'].append({'x': ts, 'y': entry.value})

    # Process FeatureData
    for entry in feature_data_queryset:
        ts = entry.timestamp.isoformat()
        if entry.metric_type == 'temperature':
            if entry.mean is not None: plot_data['feature']['temperature_mean'].append({'x': ts, 'y': entry.mean})
            if entry.min_val is not None: plot_data['feature']['temperature_min'].append({'x': ts, 'y': entry.min_val})
            if entry.max_val is not None: plot_data['feature']['temperature_max'].append({'x': ts, 'y': entry.max_val})
            if entry.median is not None: plot_data['feature']['temperature_median'].append({'x': ts, 'y': entry.median})
            if entry.std_dev is not None: plot_data['feature']['temperature_std'].append({'x': ts, 'y': entry.std_dev})
            if entry.data_range is not None: plot_data['feature']['temperature_range'].append({'x': ts, 'y': entry.data_range})
        if entry.metric_type == 'current':
            if entry.mean is not None: plot_data['feature']['current_mean'].append({'x': ts, 'y': entry.mean})
            if entry.min_val is not None: plot_data['feature']['current_min'].append({'x': ts, 'y': entry.min_val})
            if entry.max_val is not None: plot_data['feature']['current_max'].append({'x': ts, 'y': entry.max_val})
            if entry.median is not None: plot_data['feature']['current_median'].append({'x': ts, 'y': entry.median})
            if entry.std_dev is not None: plot_data['feature']['current_std'].append({'x': ts, 'y': entry.std_dev})
            if entry.data_range is not None: plot_data['feature']['current_range'].append({'x': ts, 'y': entry.data_range})
        if entry.metric_type == 'torque':
            if entry.mean is not None: plot_data['feature']['torque_mean'].append({'x': ts, 'y': entry.mean})
            if entry.min_val is not None: plot_data['feature']['torque_min'].append({'x': ts, 'y': entry.min_val})
            if entry.max_val is not None: plot_data['feature']['torque_max'].append({'x': ts, 'y': entry.max_val})
            if entry.median is not None: plot_data['feature']['torque_median'].append({'x': ts, 'y': entry.median})
            if entry.std_dev is not None: plot_data['feature']['torque_std'].append({'x': ts, 'y': entry.std_dev})
            if entry.data_range is not None: plot_data['feature']['torque_range'].append({'x': ts, 'y': entry.data_range})

    # Process PredictionData
    for entry in prediction_data_queryset:
        ts = entry.timestamp.isoformat()
        if entry.metric_type == 'temperature':
            if entry.predicted_value is not None: plot_data['prediction']['temperature_predicted_value'].append({'x': ts, 'y': entry.predicted_value})
            if entry.anomaly_score is not None: plot_data['prediction']['temperature_anomaly_score'].append({'x': ts, 'y': entry.anomaly_score})
            if entry.rul_hours is not None: plot_data['prediction']['temperature_rul_hours'].append({'x': ts, 'y': entry.rul_hours})
        if entry.metric_type == 'current':
            if entry.predicted_value is not None: plot_data['prediction']['current_predicted_value'].append({'x': ts, 'y': entry.predicted_value})
            if entry.anomaly_score is not None: plot_data['prediction']['current_anomaly_score'].append({'x': ts, 'y': entry.anomaly_score})
            if entry.rul_hours is not None: plot_data['prediction']['current_rul_hours'].append({'x': ts, 'y': entry.rul_hours})
        if entry.metric_type == 'torque':
            if entry.predicted_value is not None: plot_data['prediction']['torque_predicted_value'].append({'x': ts, 'y': entry.predicted_value})
            if entry.anomaly_score is not None: plot_data['prediction']['torque_anomaly_score'].append({'x': ts, 'y': entry.anomaly_score})
            if entry.rul_hours is not None: plot_data['prediction']['torque_rul_hours'].append({'x': ts, 'y': entry.rul_hours})

    return plot_data

def get_latest_plot_data_point():
    """
    Retrieves the very latest LiveData and TwinData point and formats it.
    Useful for continuous live updates.
    """
    latest_live = LiveData.objects.order_by('-timestamp').first()
    latest_twin = TwinData.objects.order_by('-timestamp').first()
    latest_raw_temp = RawData.objects.filter(metric_type='temperature').order_by('-timestamp').first()
    latest_raw_current = RawData.objects.filter(metric_type='current').order_by('-timestamp').first()
    latest_raw_torque = RawData.objects.filter(metric_type='torque').order_by('-timestamp').first()
    latest_feature_temp = FeatureData.objects.filter(metric_type='temperature').order_by('-timestamp').first()
    latest_feature_current = FeatureData.objects.filter(metric_type='current').order_by('-timestamp').first()
    latest_feature_torque = FeatureData.objects.filter(metric_type='torque').order_by('-timestamp').first()
    latest_prediction_temp = PredictionData.objects.filter(metric_type='temperature').order_by('-timestamp').first()
    latest_prediction_current = PredictionData.objects.filter(metric_type='current').order_by('-timestamp').first()
    latest_prediction_torque = PredictionData.objects.filter(metric_type='torque').order_by('-timestamp').first()

    data_point = {
        'live': {},
        'twin': {},
        'raw': {},
        'feature': {},
        'prediction': {}
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

    if latest_raw_temp:
        ts = latest_raw_temp.timestamp.isoformat()
        if latest_raw_temp.value is not None: data_point['raw']['temperature'] = {'x': ts, 'y': latest_raw_temp.value}
    if latest_raw_current:
        ts = latest_raw_current.timestamp.isoformat()
        if latest_raw_current.value is not None: data_point['raw']['current'] = {'x': ts, 'y': latest_raw_current.value}
    if latest_raw_torque:
        ts = latest_raw_torque.timestamp.isoformat()
        if latest_raw_torque.value is not None: data_point['raw']['torque'] = {'x': ts, 'y': latest_raw_torque.value}

    if latest_feature_temp:
        ts = latest_feature_temp.timestamp.isoformat()
        if latest_feature_temp.mean is not None: data_point['feature']['temperature_mean'] = {'x': ts, 'y': latest_feature_temp.mean}
        if latest_feature_temp.min_val is not None: data_point['feature']['temperature_min'] = {'x': ts, 'y': latest_feature_temp.min_val}
        if latest_feature_temp.max_val is not None: data_point['feature']['temperature_max'] = {'x': ts, 'y': latest_feature_temp.max_val}
        if latest_feature_temp.median is not None: data_point['feature']['temperature_median'] = {'x': ts, 'y': latest_feature_temp.median}
        if latest_feature_temp.std_dev is not None: data_point['feature']['temperature_std'] = {'x': ts, 'y': latest_feature_temp.std_dev}
        if latest_feature_temp.data_range is not None: data_point['feature']['temperature_range'] = {'x': ts, 'y': latest_feature_temp.data_range}
    if latest_feature_current:
        ts = latest_feature_current.timestamp.isoformat()
        if latest_feature_current.mean is not None: data_point['feature']['current_mean'] = {'x': ts, 'y': latest_feature_current.mean}
        if latest_feature_current.min_val is not None: data_point['feature']['current_min'] = {'x': ts, 'y': latest_feature_current.min_val}
        if latest_feature_current.max_val is not None: data_point['feature']['current_max'] = {'x': ts, 'y': latest_feature_current.max_val}
        if latest_feature_current.median is not None: data_point['feature']['current_median'] = {'x': ts, 'y': latest_feature_current.median}
        if latest_feature_current.std_dev is not None: data_point['feature']['current_std'] = {'x': ts, 'y': latest_feature_current.std_dev}
        if latest_feature_current.data_range is not None: data_point['feature']['current_range'] = {'x': ts, 'y': latest_feature_current.data_range}
    if latest_feature_torque:
        ts = latest_feature_torque.timestamp.isoformat()
        if latest_feature_torque.mean is not None: data_point['feature']['torque_mean'] = {'x': ts, 'y': latest_feature_torque.mean}
        if latest_feature_torque.min_val is not None: data_point['feature']['torque_min'] = {'x': ts, 'y': latest_feature_torque.min_val}
        if latest_feature_torque.max_val is not None: data_point['feature']['torque_max'] = {'x': ts, 'y': latest_feature_torque.max_val}
        if latest_feature_torque.median is not None: data_point['feature']['torque_median'] = {'x': ts, 'y': latest_feature_torque.median}
        if latest_feature_torque.std_dev is not None: data_point['feature']['torque_std'] = {'x': ts, 'y': latest_feature_torque.std_dev}
        if latest_feature_torque.data_range is not None: data_point['feature']['torque_range'] = {'x': ts, 'y': latest_feature_torque.data_range}

    if latest_prediction_temp:
        ts = latest_prediction_temp.timestamp.isoformat()
        if latest_prediction_temp.predicted_value is not None: data_point['prediction']['temperature_predicted_value'] = {'x': ts, 'y': latest_prediction_temp.predicted_value}
        if latest_prediction_temp.anomaly_score is not None: data_point['prediction']['temperature_anomaly_score'] = {'x': ts, 'y': latest_prediction_temp.anomaly_score}
        if latest_prediction_temp.rul_hours is not None: data_point['prediction']['temperature_rul_hours'] = {'x': ts, 'y': latest_prediction_temp.rul_hours}
    if latest_prediction_current:
        ts = latest_prediction_current.timestamp.isoformat()
        if latest_prediction_current.predicted_value is not None: data_point['prediction']['current_predicted_value'] = {'x': ts, 'y': latest_prediction_current.predicted_value}
        if latest_prediction_current.anomaly_score is not None: data_point['prediction']['current_anomaly_score'] = {'x': ts, 'y': latest_prediction_current.anomaly_score}
        if latest_prediction_current.rul_hours is not None: data_point['prediction']['current_rul_hours'] = {'x': ts, 'y': latest_prediction_current.rul_hours}
    if latest_prediction_torque:
        ts = latest_prediction_torque.timestamp.isoformat()
        if latest_prediction_torque.predicted_value is not None: data_point['prediction']['torque_predicted_value'] = {'x': ts, 'y': latest_prediction_torque.predicted_value}
        if latest_prediction_torque.anomaly_score is not None: data_point['prediction']['torque_anomaly_score'] = {'x': ts, 'y': latest_prediction_torque.anomaly_score}
        if latest_prediction_torque.rul_hours is not None: data_point['prediction']['torque_rul_hours'] = {'x': ts, 'y': latest_prediction_torque.rul_hours}


    return data_point

    # Helper function for serializing datetime objects in nested dictionaries/lists
def to_serializable_dict(obj):
    """
    Recursively converts datetime objects in a dictionary/list
    to ISO 8601 strings for JSON serialization.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: to_serializable_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_serializable_dict(elem) for elem in obj]
    return obj