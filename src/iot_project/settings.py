# Path: src/iot_project/settings.py
import os
from pathlib import Path
import sys # Import sys for stderr to ensure output is visible in Docker logs

print("DJANGO SETTINGS LOADED")
BASE_DIR = Path(__file__).resolve().parent.parent

# Secret key for Django project, read from environment variable or use a fallback
SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'your-fallback-key')
# Debug mode, read from environment variable and convert to boolean
DEBUG = os.getenv('DJANGO_DEBUG', 'True') == 'True'
# Allowed hosts for the Django application, read from environment variable
ALLOWED_HOSTS = os.getenv('DJANGO_ALLOWED_HOSTS', 'localhost').split(',') + []
# Maximum memory size for uploaded data
DATA_UPLOAD_MAX_MEMORY_SIZE = 26214400
# Maximum number of fields allowed in a form submission
DATA_UPLOAD_MAX_NUMBER_FIELDS = 500000
# Enable timezone support
USE_TZ = True
# Set the default timezone for the project
TIME_ZONE = 'Europe/Berlin' 

# List of installed Django applications
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'channels', # Django Channels for WebSocket support
    'iot_app',   # Your custom IoT application
]

# Middleware classes for request processing
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware', # For serving static files efficiently
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

# Database configuration
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('DB_NAME', 'iot_db'),          # Read from DB_NAME environment variable
        'USER': os.getenv('DB_USER', 'iot_user'),        # Read from DB_USER environment variable
        'PASSWORD': os.getenv('DB_PASSWORD', 'sicherespasswort'), # Read from DB_PASSWORD environment variable
        'HOST': os.getenv('DB_HOST', 'db'),              # Read from DB_HOST environment variable
        'PORT': os.getenv('DB_PORT', '5432'),            # Read from DB_PORT environment variable
    }
}


print(f"DEBUG: Database settings being used: {DATABASES['default']}", file=sys.stderr)


# URL for static files
STATIC_URL = '/static/'

# Directory where static files will be collected for deployment
STATIC_ROOT = BASE_DIR / 'staticfiles'

# Additional directories to search for static files
STATICFILES_DIRS = [BASE_DIR / 'iot_app' / 'static']

# Storage backend for static files, optimized for WhiteNoise
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

# URL for media files
MEDIA_URL = '/media/'

# Directory where uploaded media files will be stored
MEDIA_ROOT = BASE_DIR / 'media'

# Root URL configuration for the project
ROOT_URLCONF = 'iot_project.urls'

# Template engine configuration
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'iot_app' / 'templates'], # Directory for project-wide templates
        'APP_DIRS': True, # Enable template loading from installed apps
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    }
]

# WSGI application entry point
WSGI_APPLICATION = 'iot_project.wsgi.application'
# ASGI application entry point for Django Channels
ASGI_APPLICATION = 'iot_project.asgi.application'

# Redis host and port for Channels layer, read from environment variables
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Channel layer configuration using Redis
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {"hosts": [(REDIS_HOST, REDIS_PORT)]},
    },
}

# MQTT Broker Host for external connections 
#DOCKER_MQTT_BROKER_HOST = "192.168.0.20"

# MQTT Broker Host for internal Docker communication 
DOCKER_MQTT_BROKER_HOST = os.environ.get('DOCKER_MQTT_BROKER_HOST', 'mqtt_broker')

# MQTT Broker Port
MQTT_BROKER_PORT = 1883



print(
    f"DEBUG MQTT BROKER: {DOCKER_MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}",
    file=sys.stderr
)

# MQTT Topics for different data streams
MQTT_TOPIC_LIVE = os.environ.get('MQTT_TOPIC_LIVE', 'iot/motor/live')           # erste Parameter 'MQTT_TOPIC_LIVE' ist der Name der Umgebungsvariablen, nach der gesucht wird.    
                                                                                # zweite Parameter 'iot/motor/live' ist der Standardwert (Default-Wert)
MQTT_TOPIC_TWIN = os.environ.get('MQTT_TOPIC_TWIN', 'iot/motor/twin')

TOPIC_RAW_TEMPERATURE = "raw/temperature"
TOPIC_RAW_CURRENT = "raw/current"
TOPIC_RAW_TORQUE = "raw/torque"

TOPIC_RAW_VIBRATION_VIN = "Sensor/vin/vibration_raw"
TOPIC_RAW_GPIO_RPM = "Sensor/gpio/rpm"

FEATURE_TOPIC_TEMPERATURE = "feature/temperature"
FEATURE_TOPIC_CURRENT = "feature/current"
FEATURE_TOPIC_TORQUE = "feature/torque"

PREDICTION_TOPIC_TEMPERATURE = "prediction/temperature"
PREDICTION_TOPIC_CURRENT = "prediction/current"
PREDICTION_TOPIC_TORQUE = "prediction/torque"

MQTT_TOPIC_MALFUNCTION_INFO = os.environ.get('MQTT_TOPIC_MALFUNCTION_INFO', 'iot/motor/malfunction/info')
MQTT_TOPIC_MALFUNCTION_WARNING = os.environ.get('MQTT_TOPIC_MALFUNCTION_WARNING', 'iot/motor/malfunction/warning')
MQTT_TOPIC_MALFUNCTION_ERROR = os.environ.get('MQTT_TOPIC_MALFUNCTION_ERROR', 'iot/motor/malfunction/error')

# Wenn keine neuen Daten innerhalb dieser Zeitspanne empfangen wurden,
# werden die Werte auf dem Dashboard als "nicht verfügbar" (-) angezeigt.
DATA_FRESHNESS_THRESHOLD_SECONDS = 10

# Default percentage deviation threshold between live and twin data that triggers a warning
MQTT_DEVIATION_THRESHOLD_PERCENT = 80.0

# Default primary key field type for models
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'