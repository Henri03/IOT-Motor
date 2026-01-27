# IOT_PROJECT/src/iot_app/admin.py
from django.contrib import admin
from .models import MotorInfo, LiveData, TwinData, ReferenceRun, MalfunctionLog, RawData, FeatureData, PredictionData

@admin.register(MotorInfo)
class MotorInfoAdmin(admin.ModelAdmin):
    list_display = ('name', 'model', 'identification', 'location', 'commissioning_date', 'cycles', 'operating_mode')
    search_fields = ('name', 'model', 'identification', 'location')

@admin.register(LiveData)
class LiveDataAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'current', 'voltage', 'rpm','vibration', 'temp', 'torque', 'run_time')
    list_filter = ('timestamp',)
    readonly_fields = ('timestamp',)

@admin.register(TwinData)
class TwinDataAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'current', 'voltage', 'rpm', 'vibration', 'temp', 'torque', 'run_time')
    list_filter = ('timestamp',)
    search_fields = ('current',)
    readonly_fields = ('timestamp',)

@admin.register(ReferenceRun)
class ReferenceRunAdmin(admin.ModelAdmin):
    list_display = ('name', 'timestamp', 'is_valid', 'current', 'voltage', 'rpm', 'vibration', 'temp', 'torque', 'run_time')
    list_filter = ('is_valid',)
    search_fields = ('name',)

@admin.register(MalfunctionLog)
class MalfunctionLogAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'message_type', 'description', 'motor_state', 'emergency_stop_active')
    list_filter = ('message_type', 'motor_state', 'emergency_stop_active')
    search_fields = ('description',)
    readonly_fields = ('timestamp',)

@admin.register(RawData)
class RawDataAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'metric_type', 'value')
    list_filter = ('metric_type', 'timestamp',)
    search_fields = ('metric_type',)
    readonly_fields = ('timestamp',)

@admin.register(FeatureData)
class FeatureDataAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'metric_type', 'mean', 'min_val', 'max_val', 'median', 'std_dev', 'data_range')
    list_filter = ('metric_type', 'timestamp',)
    search_fields = ('metric_type',)
    readonly_fields = ('timestamp',)

@admin.register(PredictionData)
class PredictionDataAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'metric_type', 'status_value')
    list_filter = ('metric_type', 'timestamp',)
    search_fields = ('metric_type',)
    readonly_fields = ('timestamp',)    