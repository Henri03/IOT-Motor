# path: src/iot_app/urls.py
from django.urls import path
from . import views

app_name = 'iot_app'

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'), 
    path('dashboard/', views.dashboard_view, name='dashboard_detail'), # Allows /dashboard/ as URL
    path('malfunction-log/', views.malfunction_log_view, name='malfunction_log'), 
    path('acknowledge-log/<int:log_id>/', views.acknowledge_log, name='acknowledge_log'),
    path('delete-log/<int:log_id>/', views.delete_log, name='delete_log'),
]