# IOT_PROJECT/src/iot_project/asgi.py
import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'iot_project.settings')

# Initialisiere die Django ASGI-Anwendung frühzeitig, um sicherzustellen,
# dass die AppRegistry gefüllt ist, bevor Code importiert wird,
# der ORM-Modelle importieren könnte.
django_asgi_app = get_asgi_application()

# Importiere das Routing erst NACHDEM die Django-Anwendung initialisiert wurde.
import iot_app.routing

application = ProtocolTypeRouter({
    "http": django_asgi_app,
    "websocket": AuthMiddlewareStack(
        URLRouter(
            iot_app.routing.websocket_urlpatterns
        )
    ),
})