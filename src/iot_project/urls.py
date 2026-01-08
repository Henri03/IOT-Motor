# src/iot_project/urls.py

from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('iot_app.urls')), # Include your app's URLs
]