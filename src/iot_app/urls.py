# IOT_PROJECT/src/iot_app/urls.py
from django.urls import path
from . import views

app_name = 'iot_app'

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'), 
    path('dashboard/', views.dashboard_view, name='dashboard_detail'), # Erlaubt weiterhin /dashboard/ als URL
    path('malfunction_log/', views.malfunction_log_view, name='malfunction_log'), 
]