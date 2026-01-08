import paho.mqtt.client as mqtt
import json
import os
import datetime
import argparse # NEU: Für Kommandozeilenargumente

# MQTT Broker Einstellungen
# NEU: Verwende MQTT_BROKER_HOST für externe Verbindungen (manuelle Ausführung)
BROKER_ADDRESS = os.getenv('MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
TOPIC_WARNING = os.getenv('MQTT_TOPIC_MALFUNCTION_WARNING', 'iot/motor/malfunction/warning')

# Zähler für abwechselnde Nachrichten (wird nur verwendet, wenn keine Beschreibung übergeben wird)
message_counter = 0

def generate_warning_message(custom_description=None, custom_motor_state=None, custom_emergency_stop=None):
    """Generiert eine Warnmeldung, optional mit benutzerdefinierter Beschreibung."""
    global message_counter
    
    if custom_description:
        description = custom_description
    else:
        # Standard-Logik für abwechselnde Nachrichten
        if message_counter % 2 == 0:
            description = "Abweichung: Werte von Twin und echtem Motor."
        else:
            description = "Motor zeigt ungewöhnliches Verhalten."
        message_counter += 1

    motor_state = custom_motor_state if custom_motor_state is not None else "warning"
    emergency_stop = custom_emergency_stop if custom_emergency_stop is not None else False

    data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "message_type": "WARNING",
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
    warning_payload = generate_warning_message(args.description, args.motor_state, args.emergency_stop)
    
    client.loop_start() # Starte den Thread für die Netzwerkkommunikation
    client.publish(TOPIC_WARNING, json.dumps(warning_payload))
    print(f"Veröffentlichte Warnmeldung auf {TOPIC_WARNING}: {warning_payload}")
    
    time.sleep(1) # Kurze Pause, um sicherzustellen, dass die Nachricht gesendet wird
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    # NEU: Argument-Parser
    parser = argparse.ArgumentParser(description="Veröffentlicht eine einzelne Dummy-Warnmeldung auf MQTT.")
    parser.add_argument('--description', type=str, help='Benutzerdefinierte Beschreibung für die Warnmeldung.')
    parser.add_argument('--motor-state', type=str, help='Benutzerdefinierter Motorzustand (z.B. "warning", "overload").')
    parser.add_argument('--emergency-stop', action='store_true', help='Setzt emergency_stop_active auf True.')
    args = parser.parse_args()
    
    publish_data(args)