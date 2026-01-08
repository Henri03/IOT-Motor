import paho.mqtt.client as mqtt
import time
import json
import random
import datetime
import os

# MQTT Broker settings
BROKER_ADDRESS = os.getenv('DOCKER_MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
TOPIC_LIVE = "iot/motor/live"
TOPIC_TWIN = "iot/motor/twin"

def generate_live_data():
    """Generates realistic live sensor data."""
    current = round(random.uniform(10.0, 25.0) + random.gauss(0, 0.5), 2)
    voltage = round(random.uniform(220.0, 240.0) + random.gauss(0, 1.0), 2)
    rpm = round(random.uniform(1400.0, 1500.0) + random.gauss(0, 5.0), 2)
    vibration = round(random.uniform(0.5, 5.0) + random.gauss(0, 0.2), 2)
    temp = round(random.uniform(30.0, 60.0) + random.gauss(0, 1.0), 2)
    torque = round(random.uniform(50.0, 80.0) + random.gauss(0, 2.0), 2)
    run_time = round(random.uniform(100.0, 5000.0) + random.gauss(0, 10.0), 2)

    malfunction = False
    motor_state = "normal"
    emergency_stop = False
    
    # Introduce occasional "anomaly" for live data (e.g., higher current)
    if random.random() < 0.05: 
        anomaly_type = random.choice(['current', 'vibration', 'temp', 'rpm', 'torque'])
        if anomaly_type == 'current':
            current += random.uniform(8.0, 15.0)
        elif anomaly_type == 'vibration':
            vibration += random.uniform(5.0, 15.0)
        elif anomaly_type == 'temp':
            temp += random.uniform(15.0, 30.0)
        elif anomaly_type == 'rpm':
            rpm += random.uniform(100.0, 300.0)
        elif anomaly_type == 'torque':
            torque += random.uniform(20.0, 40.0)
        
        malfunction = True
        motor_state = "warning"
        if random.random() < 0.1:
            emergency_stop = True
            motor_state = "emergency_stop"

    data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "current": current,
        "voltage": voltage,
        "rpm": rpm,
        "vibration": vibration,
        "temp": temp,
        "torque": torque,
        "run_time": run_time,
        "malfunction": malfunction,
        "motor_state": motor_state,
        "emergency_stop": emergency_stop,
    }
    return data

def generate_twin_data(live_data_payload):
    """Generates expected data from a behavioral model (digital twin)."""
    
    # Base values for twin data
    base_current = random.uniform(15.0, 20.0)
    base_voltage = random.uniform(225.0, 235.0)
    base_rpm = random.uniform(1440.0, 1460.0)
    base_torque = random.uniform(60.0, 75.0)
    base_run_time = random.uniform(100.0, 5000.0)
    base_vibration = random.uniform(1.0, 3.0)
    base_temp = random.uniform(40.0, 55.0)

    # Adjust twin values based on live values to simulate realistic deviations
    current = round(base_current + random.gauss(0, 0.2), 2)
    if live_data_payload.get('current') is not None and live_data_payload['current'] > 30:
        current = round(random.uniform(18.0, 22.0) + random.gauss(0, 0.2), 2)

    voltage = round(base_voltage + random.gauss(0, 0.5), 2)
    rpm = round(base_rpm + random.gauss(0, 2.0), 2)
    torque = round(base_torque + random.gauss(0, 1.0), 2)
    run_time = round(base_run_time + random.gauss(0, 5.0), 2)

    vibration = round(base_vibration + random.gauss(0, 0.1), 2)
    if live_data_payload.get('vibration') is not None and live_data_payload['vibration'] > 10:
        vibration = round(random.uniform(3.0, 6.0) + random.gauss(0, 0.1), 2)

    temp = round(base_temp + random.gauss(0, 0.5), 2)
    if live_data_payload.get('temp') is not None and live_data_payload['temp'] > 70:
        temp = round(random.uniform(50.0, 70.0) + random.gauss(0, 0.5), 2)

    data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "current": current,
        "voltage": voltage,
        "rpm": rpm,
        "vibration": vibration,
        "temp": temp,
        "torque": torque,
        "run_time": run_time,
    }
    return data

def on_connect(client, userdata, flags, rc):
    """Callback function for MQTT connection."""
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Connection failed, return code {rc}")

def publish_data():
    """Initializes MQTT client and publishes dummy data."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2) 
    client.on_connect = on_connect
    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    client.loop_start()

    print(f"Publishing dummy data to {TOPIC_LIVE} and {TOPIC_TWIN} for the motor...")

    try:
        while True:
            live_data_payload = generate_live_data()
            twin_data_payload = generate_twin_data(live_data_payload)

            client.publish(TOPIC_LIVE, json.dumps(live_data_payload))
            print(f"Published Live Data: {live_data_payload}")

            client.publish(TOPIC_TWIN, json.dumps(twin_data_payload))
            print(f"Published Twin Data: {twin_data_payload}")

            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Publisher stopped.")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    publish_data()