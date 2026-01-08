import paho.mqtt.client as mqtt
import json
import os
import datetime
import argparse # NEU: Für Kommandozeilenargumente

# MQTT Broker Einstellungen
# NEU: Verwende MQTT_BROKER_HOST für externe Verbindungen (manuelle Ausführung)
BROKER_ADDRESS = os.getenv('MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
TOPIC_INFO = os.getenv('MQTT_TOPIC_MALFUNCTION_INFO', 'iot/motor/malfunction/info')

# Zähler für abwechselnde Nachrichten (wird nur verwendet, wenn keine Beschreibung übergeben wird)
message_counter = 0

def generate_info_message(custom_description=None, custom_motor_state=None, custom_emergency_stop=None):
    """Generiert eine Info-Meldung, optional mit benutzerdefinierter Beschreibung."""
    global message_counter
    
    if custom_description:
        description = custom_description
    else:
        # Standard-Logik für abwechselnde Nachrichten
        if message_counter % 2 == 0:
            description = "Motor fährt ein."
        else:
            description = "Motor fährt aus."
        message_counter += 1

    motor_state = custom_motor_state if custom_motor_state is not None else "running"
    emergency_stop = custom_emergency_stop if custom_emergency_stop is not None else False

    data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "message_type": "INFO",
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
    info_payload = generate_info_message(args.description, args.motor_state, args.emergency_stop)
    
    client.loop_start() # Starte den Thread für die Netzwerkkommunikation
    client.publish(TOPIC_INFO, json.dumps(info_payload))
    print(f"Veröffentlichte Info-Meldung auf {TOPIC_INFO}: {info_payload}")
    
    time.sleep(1) # Kurze Pause, um sicherzustellen, dass die Nachricht gesendet wird
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    # NEU: Argument-Parser
    parser = argparse.ArgumentParser(description="Veröffentlicht eine einzelne Dummy-Info-Meldung auf MQTT.")
    parser.add_argument('--description', type=str, help='Benutzerdefinierte Beschreibung für die Info-Meldung.')
    parser.add_argument('--motor-state', type=str, help='Benutzerdefinierter Motorzustand (z.B. "running", "idle").')
    parser.add_argument('--emergency-stop', action='store_true', help='Setzt emergency_stop_active auf True.')
    args = parser.parse_args()
    
    publish_data(args)