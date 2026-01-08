import paho.mqtt.client as mqtt
import json
import os
import datetime
import argparse # NEU: Für Kommandozeilenargumente

# MQTT Broker Einstellungen
# NEU: Verwende MQTT_BROKER_HOST für externe Verbindungen (manuelle Ausführung)
BROKER_ADDRESS = os.getenv('MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
TOPIC_ERROR = os.getenv('MQTT_TOPIC_MALFUNCTION_ERROR', 'iot/motor/malfunction/error')

# Zähler für abwechselnde Nachrichten (wird nur verwendet, wenn keine Beschreibung übergeben wird)
message_counter = 0

def generate_error_message(custom_description=None, custom_motor_state=None, custom_emergency_stop=None):
    """Generiert eine Fehlermeldung, optional mit benutzerdefinierter Beschreibung."""
    global message_counter
    
    if custom_description:
        description = custom_description
    else:
        # Standard-Logik für abwechselnde Nachrichten
        if message_counter % 2 == 0:
            description = "Kritische Störung: Motor defekt."
            emergency_stop = True # Standardmäßig Not-Aus bei diesem Fehler
        else:
            description = "Motor überhitzt, Notabschaltung erforderlich."
            emergency_stop = True # Standardmäßig Not-Aus bei diesem Fehler
        message_counter += 1

    motor_state = custom_motor_state if custom_motor_state is not None else "critical"
    emergency_stop = custom_emergency_stop if custom_emergency_stop is not None else emergency_stop # Behält den Standard bei, falls nicht explizit gesetzt

    data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "message_type": "ERROR",
        "description": description,
        "motor_state": motor_state,
        "emergency_stop_active": emergency_stop,
    }
    return data

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Verbunden mit MQTT Broker!")
    else:
        print(f"Verbindung fehlgeschlagen, Rückgabecode {rc}")

def publish_data(args): # NEU: args-Parameter
    client = mqtt.Client()
    client.on_connect = on_connect
    try:
        client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    except Exception as e:
        print(f"Fehler beim Verbinden mit MQTT Broker {BROKER_ADDRESS}:{BROKER_PORT}: {e}")
        return

    # NEU: Nur eine Nachricht senden und dann beenden
    error_payload = generate_error_message(args.description, args.motor_state, args.emergency_stop)
    
    client.loop_start() # Starte den Thread für die Netzwerkkommunikation
    client.publish(TOPIC_ERROR, json.dumps(error_payload))
    print(f"Veröffentlichte Fehlermeldung auf {TOPIC_ERROR}: {error_payload}")
    
    time.sleep(1) # Kurze Pause, um sicherzustellen, dass die Nachricht gesendet wird
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    # NEU: Argument-Parser
    parser = argparse.ArgumentParser(description="Veröffentlicht eine einzelne Dummy-Fehlermeldung auf MQTT.")
    parser.add_argument('--description', type=str, help='Benutzerdefinierte Beschreibung für die Fehlermeldung.')
    parser.add_argument('--motor-state', type=str, help='Benutzerdefinierter Motorzustand (z.B. "critical", "stopped").')
    parser.add_argument('--emergency-stop', action='store_true', help='Setzt emergency_stop_active auf True.')
    args = parser.parse_args()
    
    publish_data(args)