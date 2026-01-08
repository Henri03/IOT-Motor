# IOT_PROJECT/src/iot_app/models.py
from django.db import models
from django.utils import timezone

class MotorInfo(models.Model):
    """
    Model for storing static information about the motor.
    Since there is only one motor, it is retrieved via MotorInfo.objects.first().
    """
    name = models.CharField(max_length=100, verbose_name="Motor Name")
    model = models.CharField(max_length=100, verbose_name="Model")
    description = models.TextField(verbose_name="Description")
    identification = models.CharField(max_length=100, unique=True, verbose_name="Identification")
    location = models.CharField(max_length=100, verbose_name="Location")
    commissioning_date = models.DateField(null=True, blank=True, verbose_name="Commissioning Date")
    cycles = models.PositiveIntegerField(default=0, verbose_name="Operating Cycles")
    operating_mode = models.CharField(max_length=50, verbose_name="Operating Mode")

    def __str__(self):
        return f"{self.name} ({self.identification})"

    class Meta:
        verbose_name = "Motor Information"
        verbose_name_plural = "Motor Information"

class LiveData(models.Model):
    """
    Model for storing real-time sensor data of the motor.
    """
    timestamp = models.DateTimeField(auto_now_add=True, verbose_name="Timestamp")
    current = models.FloatField(null=True, blank=True, verbose_name="Current (A)")
    voltage = models.FloatField(null=True, blank=True, verbose_name="Voltage (V)")
    rpm = models.FloatField(null=True, blank=True, verbose_name="RPM")
    vibration = models.FloatField(null=True, blank=True, verbose_name="Vibration (mm/s)")
    temp = models.FloatField(null=True, blank=True, verbose_name="Temperature (°C)")
    torque = models.FloatField(null=True, blank=True, verbose_name="Torque (Nm)")
    run_time = models.FloatField(null=True, blank=True, verbose_name="Run Time (h)")

    def __str__(self):
        return f"Live Data at {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')} (Current: {self.current or 'N/A'}A)"

    class Meta:
        verbose_name = "Live Data"
        verbose_name_plural = "Live Data"
        ordering = ['-timestamp']

class TwinData(models.Model):
    """
    Model for storing expected/modeled data from the motor's digital twin.
    Attributes mirror LiveData for comparison.
    """
    timestamp = models.DateTimeField(auto_now_add=True, verbose_name="Timestamp")
    current = models.FloatField(null=True, blank=True, verbose_name="Expected Current (A)")
    voltage = models.FloatField(null=True, blank=True, verbose_name="Expected Voltage (V)")
    rpm = models.FloatField(null=True, blank=True, verbose_name="Expected RPM")
    vibration = models.FloatField(null=True, blank=True, verbose_name="Expected Vibration (mm/s)")
    temp = models.FloatField(null=True, blank=True, verbose_name="Expected Temperature (°C)")
    torque = models.FloatField(null=True, blank=True, verbose_name="Expected Torque (Nm)")
    run_time = models.FloatField(null=True, blank=True, verbose_name="Expected Run Time (h)")

    def __str__(self):
        return f"Twin Data at {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')} (Current: {self.current or 'N/A'}A)"

    class Meta:
        verbose_name = "Twin Data"
        verbose_name_plural = "Twin Data"
        ordering = ['-timestamp']

class ReferenceRun(models.Model):
    """
    Model for storing reference runs or setpoints for the motor.
    """
    name = models.CharField(max_length=100, verbose_name="Reference Run Name")
    timestamp = models.DateTimeField(auto_now_add=True, verbose_name="Timestamp")
    is_valid = models.BooleanField(default=True, verbose_name="Is Valid")
    current = models.FloatField(null=True, blank=True, verbose_name="Current (A)")
    voltage = models.FloatField(null=True, blank=True, verbose_name="Voltage (V)")
    rpm = models.FloatField(null=True, blank=True, verbose_name="RPM")
    vibration = models.FloatField(null=True, blank=True, verbose_name="Vibration (mm/s)")
    temp = models.FloatField(null=True, blank=True, verbose_name="Temperature (°C)")
    torque = models.FloatField(null=True, blank=True, verbose_name="Torque (Nm)")
    run_time = models.FloatField(null=True, blank=True, verbose_name="Run Time (h)")

    def __str__(self):
        return f"Reference Run '{self.name}' at {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"

    class Meta:
        verbose_name = "Reference Run"
        verbose_name_plural = "Reference Runs"
        ordering = ['-timestamp']

class MalfunctionLog(models.Model):
    """
    Model for storing malfunction messages and events.
    """
    MESSAGE_TYPE_CHOICES = [
        ('INFO', 'Information'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
    ]
    timestamp = models.DateTimeField(auto_now_add=True, verbose_name="Timestamp")
    message_type = models.CharField(max_length=10, choices=MESSAGE_TYPE_CHOICES, verbose_name="Message Type")
    description = models.TextField(verbose_name="Description")
    motor_state = models.CharField(max_length=50, verbose_name="Motor State")
    emergency_stop_active = models.BooleanField(default=False, verbose_name="Emergency Stop Active")

    @property
    def css_class(self):
        """Returns a CSS class based on the message type for styling."""
        return {
            'INFO': 'log-info',
            'WARNING': 'log-warning',
            'ERROR': 'log-error'
        }.get(self.message_type, '')
    
    def __str__(self):
        return f"[{self.message_type}] {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')} - {self.description}"

    class Meta:
        verbose_name = "Malfunction Log"
        verbose_name_plural = "Malfunction Logs"
        ordering = ['-timestamp']