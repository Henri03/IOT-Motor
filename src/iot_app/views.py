# path: src/iot_app/views.py
from django.shortcuts import render, get_object_or_404, redirect
from django.utils import timezone
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import json # Import json for parsing request body
from datetime import datetime

from .models import MotorInfo, LiveData, TwinData, MalfunctionLog

def dashboard_view(request):
    """
    Renders the IoT dashboard for the single motor, displaying live and twin data.
    """
    # Since there is only one motor, we simply retrieve the first (and only) motor
    motor = MotorInfo.objects.first()
    if not motor:
        return render(request, 'iot_app/no_motor_configured.html', {'message': 'Please configure a motor in the admin area first.'})

    # Retrieve latest live data
    latest_live_data = LiveData.objects.order_by('-timestamp').first()

    # Retrieve latest twin data
    latest_twin_data = TwinData.objects.order_by('-timestamp').first()

    # Prepare live data for the template
    real_motor_data = {
        "Current": {"value": latest_live_data.current if latest_live_data else None, "unit": "A"},
        "Voltage": {"value": latest_live_data.voltage if latest_live_data else None, "unit": "V"},
        "RPM": {"value": latest_live_data.rpm if latest_live_data else None, "unit": "RPM"},
        "Torque": {"value": latest_live_data.torque if latest_live_data else None, "unit": "Nm"},
        "Run_Time": {"value": latest_live_data.run_time if latest_live_data else None, "unit": "h"},
    }

    # Prepare digital twin data for the template
    digital_twin_data = {
        "Current": {"value": latest_twin_data.current if latest_twin_data else None, "unit": "A"},
        "Voltage": {"value": latest_twin_data.voltage if latest_twin_data else None, "unit": "V"},
        "RPM": {"value": latest_twin_data.rpm if latest_twin_data else None, "unit": "RPM"},
        "Torque": {"value": latest_twin_data.torque if latest_twin_data else None, "unit": "Nm"},
        "Run_Time": {"value": latest_twin_data.run_time if latest_twin_data else None, "unit": "h"},
    }

    # Simple anomaly status (can be expanded)
    anomaly_detected = False
    anomaly_message = " - "
    if latest_live_data and latest_twin_data:
        if latest_live_data.current is not None and latest_twin_data.current is not None and \
           abs(latest_live_data.current - latest_twin_data.current) > 5.0: # Example threshold
            anomaly_detected = True
            anomaly_message = "WARNING: Current deviation detected!"

    anomaly_status = {
        "detected": anomaly_detected,
        "message": anomaly_message,
    }

    context = {
        'motor_info': motor,
        'real_motor_data': real_motor_data,
        'digital_twin_data': digital_twin_data,
        'anomaly_status': anomaly_status,
        'current_year': timezone.now().year,
    }

    return render(request, 'iot_app/dashboard.html', context)

def malfunction_log_view(request):
    """
    Displays the malfunction logs for the motor.
    Only displays logs that have not been acknowledged.
    """
    # Filter for logs where 'acknowledged' is False
    logs = MalfunctionLog.objects.filter(acknowledged=False).order_by('-timestamp')[:100] # Limit to last 100 unacknowledged logs
    current_year = datetime.now().year
    context = {
        'malfunction_logs': logs,
        'current_year': current_year,
    }
    return render(request, 'iot_app/malfunction_log.html', context)

@require_POST
def acknowledge_log(request, log_id):
    """
    API endpoint to acknowledge a specific malfunction log.
    Requires POST request.
    """
    try:
        log_entry = get_object_or_404(MalfunctionLog, id=log_id)
        log_entry.acknowledged = True
        log_entry.save()
        return JsonResponse({'success': True, 'message': 'Log acknowledged successfully.'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@require_POST
def delete_log(request, log_id):
    """
    API endpoint to delete a specific malfunction log.
    Requires POST request.
    """
    try:
        log_entry = get_object_or_404(MalfunctionLog, id=log_id)
        log_entry.delete()
        return JsonResponse({'success': True, 'message': 'Log deleted successfully.'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)