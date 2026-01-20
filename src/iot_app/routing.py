# IOT_PROJECT/src/iot_app/routing.py

# Websocket
# Es konfiguriert Ihre Django Channels-Anwendung so, dass sie WebSocket-Verbindungen unter dem Pfad /ws/dashboard/ akzeptiert. 
# Wenn eine Verbindung zu diesem Pfad hergestellt wird, wird die DashboardConsumer-Klasse aus Ihrer consumers.py-Datei instanziiert und ist für die Handhabung der gesamten Kommunikation über diese spezifische WebSocket-Verbindung verantwortlich.

from django.urls import re_path
from . import consumers

websocket_urlpatterns = [
    re_path(r'ws/dashboard/$', consumers.DashboardConsumer.as_asgi()), 
]