import paho.mqtt.client as mqtt
import time
import json
import random
import datetime
import os
import statistics
from pytz import utc # Import UTC timezone
from collections import deque # Für den Datenpuffer

# MQTT Broker settings
BROKER_ADDRESS = os.getenv('DOCKER_MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))

# --- MQTT Topics ---
TOPIC_LIVE = "iot/motor/live"
TOPIC_TWIN = "iot/motor/twin"

# Raw Data Topics (matching consumer's expected topics)
TOPIC_RAW_TEMPERATURE = "raw/temperature"
TOPIC_RAW_CURRENT = "raw/current"
TOPIC_RAW_TORQUE = "raw/torque"
TOPIC_RAW_VOLTAGE = "raw/voltage"
TOPIC_RAW_VIBRATION_VIN = "Sensor/vin/vibration_raw"
TOPIC_RAW_GPIO_RPM = "Sensor/gpio/rpm"

# Feature Data Topics
TOPIC_FEATURE_TEMPERATURE = "feature/temperature"
TOPIC_FEATURE_CURRENT = "feature/current"
TOPIC_FEATURE_TORQUE = "feature/torque"

# Prediction Data Topics
TOPIC_PREDICTION_TEMPERATURE = "prediction/temperature"
TOPIC_PREDICTION_CURRENT = "prediction/current"
TOPIC_PREDICTION_TORQUE = "prediction/torque"

# --- Globale Puffer für Raw-Werte ---
# Speichert die letzten N Werte für die Durchschnittsberechnung der Twin-Daten
N_RAW_VALUES = 20 # Anzahl der letzten Raw-Werte, die für den Durchschnitt verwendet werden sollen

raw_data_buffers = {
    'temperature': deque(maxlen=N_RAW_VALUES),
    'current': deque(maxlen=N_RAW_VALUES),
    'torque': deque(maxlen=N_RAW_VALUES),
    'voltage': deque(maxlen=N_RAW_VALUES),
    'vibration': deque(maxlen=N_RAW_VALUES),
    'rpm': deque(maxlen=N_RAW_VALUES),
}

# --- MQTT Client für den Publisher (zum Senden und Empfangen) ---
publisher_client = None

def on_connect_publisher(client, userdata, flags, rc, properties):
    """Callback function for MQTT connection of the publisher."""
    if rc == 0:
        print("Publisher Connected to MQTT Broker!")
        # Abonnieren der Raw-Topics, um auf Änderungen zu reagieren
        client.subscribe(TOPIC_RAW_TEMPERATURE)
        client.subscribe(TOPIC_RAW_CURRENT)
        client.subscribe(TOPIC_RAW_TORQUE)
        client.subscribe(TOPIC_RAW_VOLTAGE)
        client.subscribe(TOPIC_RAW_VIBRATION_VIN)
        client.subscribe(TOPIC_RAW_GPIO_RPM)
        print("Publisher subscribed to raw data topics.")
    else:
        print(f"Publisher Connection failed, return code {rc}")

def on_message_publisher(client, userdata, msg):
    """Callback function for MQTT messages received by the publisher."""
    try:
        payload = json.loads(msg.payload.decode())
        value = payload.get('value')
        if value is None:
            print(f"Received message on {msg.topic} without 'value' key: {payload}")
            return

        # Speichern der empfangenen Raw-Werte in den Puffern
        if msg.topic == TOPIC_RAW_TEMPERATURE:
            raw_data_buffers['temperature'].append(value)
        elif msg.topic == TOPIC_RAW_CURRENT:
            raw_data_buffers['current'].append(value)
        elif msg.topic == TOPIC_RAW_TORQUE:
            raw_data_buffers['torque'].append(value)
        elif msg.topic == TOPIC_RAW_VOLTAGE:
            raw_data_buffers['voltage'].append(value)
        elif msg.topic == TOPIC_RAW_VIBRATION_VIN:
            # Vibration VIN sendet int, umwandeln für Durchschnittsberechnung
            raw_data_buffers['vibration'].append(value / 10000.0) # Korrekte Skalierung
        elif msg.topic == TOPIC_RAW_GPIO_RPM:
            raw_data_buffers['rpm'].append(value)
        
        # print(f"Publisher received and buffered: {msg.topic} -> {value}") # Kann bei Bedarf aktiviert werden

    except json.JSONDecodeError:
        print(f"Publisher: Failed to decode JSON from message: {msg.payload}")
    except Exception as e:
        print(f"Publisher: Error processing message on topic {msg.topic}: {e}")

def get_average_from_buffer(metric_name, default_value, noise_std_dev=0.1):
    """
    Calculates the average of the buffered raw values for a given metric,
    adding a small amount of Gaussian noise.
    """
    buffer = raw_data_buffers.get(metric_name)
    if buffer and len(buffer) > 0:
        # Füge Rauschen zum Durchschnitt hinzu
        return statistics.mean(buffer) + random.gauss(0, noise_std_dev)
    # Wenn der Puffer leer ist, verwende den Standardwert mit Rauschen
    return default_value + random.gauss(0, noise_std_dev)

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
        anomaly_type = random.choice(['current', 'vibration', 'temp', 'rpm', 'torque', 'voltage']) # Added voltage to anomalies
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
        elif anomaly_type == 'voltage': # Anomaly for voltage
            voltage += random.uniform(10.0, 20.0)
        
        malfunction = True
        motor_state = "warning"
        if random.random() < 0.1:
            emergency_stop = True
            motor_state = "emergency_stop"

    # Ensure timestamp is UTC and timezone-aware
    timestamp_utc = datetime.datetime.now(utc).isoformat()

    data = {
        "timestamp": timestamp_utc,
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

def generate_twin_data():
    """
    Generates expected data from a behavioral model (digital twin).
    Twin data for existing raw features (temp, current, torque, voltage, vibration, rpm)
    are based on the average of the last N raw values from the buffers.
    For other features (run_time), they are still randomly generated.
    """
    
    # Use average of raw data from buffers for twin values
    # Default values are used if buffers are not yet filled
    current = round(get_average_from_buffer('current', random.uniform(15.0, 20.0), noise_std_dev=0.05), 2)
    voltage = round(get_average_from_buffer('voltage', random.uniform(225.0, 235.0), noise_std_dev=0.1), 2)
    rpm = round(get_average_from_buffer('rpm', random.uniform(1440.0, 1460.0), noise_std_dev=0.5), 2)
    # Angepasster default_value für Vibration, um den Bereich der Live-Daten zu matchen
    # WICHTIG: Der hier berechnete Wert ist noch im Bereich 0.5-5.0
    vibration_calculated = get_average_from_buffer('vibration', random.uniform(0.5, 5.0), noise_std_dev=0.01) 
    # Für die Darstellung im Plot muss der Twin-Vibrationswert ebenfalls mit 10000 multipliziert werden
    vibration = round(vibration_calculated * 10000, 2) 

    temp = round(get_average_from_buffer('temperature', random.uniform(40.0, 55.0), noise_std_dev=0.1), 2)
    torque = round(get_average_from_buffer('torque', random.uniform(60.0, 75.0), noise_std_dev=0.2), 2)
    
    # For run_time, which doesn't have a direct raw counterpart in your current setup,
    # we keep it randomly generated.
    run_time = round(random.uniform(100.0, 5000.0) + random.gauss(0, 5.0), 2)

    # Ensure timestamp is UTC and timezone-aware
    timestamp_utc = datetime.datetime.now(utc).isoformat()

    data = {
        "timestamp": timestamp_utc,
        "current": current,
        "voltage": voltage,
        "rpm": rpm,
        "vibration": vibration, # Dieser Wert ist nun mit 10000 multipliziert
        "temp": temp,
        "torque": torque,
        "run_time": run_time,
    }
    return data

def generate_feature_data_dummy(metric_value, timestamp):
    """
    Generates dummy feature data based on a single metric value and includes a timestamp.
    The min and max values are given a slightly larger span as requested.
    """
    base_val = metric_value
    
    # Dummy features, with a wider span for min/max values
    mean_val = round(base_val * random.uniform(0.98, 1.02), 2)
    min_val = round(base_val * random.uniform(0.8, 0.95), 2) # Wider span
    max_val = round(base_val * random.uniform(1.05, 1.2), 2) # Wider span
    median_val = round(base_val * random.uniform(0.99, 1.01), 2)
    std_dev_val = round(base_val * random.uniform(0.01, 0.05), 2) # Slightly varied std dev
    range_val = round(max_val - min_val, 2)

    features = {
        "timestamp": timestamp,
        "mean": mean_val,
        "min": min_val,
        "max": max_val,
        "median": median_val,
        "std": std_dev_val,
        "range": range_val,
    }
    return features

def generate_prediction_data_dummy(metric_value):
    """
    Generates dummy prediction data for a metric.
    It sends a status (-1 for 'bad', 1 for 'good').
    The publisher should send -1 (bad value) rarely.
    """
    status = 1  # Assume good by default

    # Introduce a 'bad' prediction (-1) rarely, as requested.
    # The probability of sending -1 is reduced.
    if random.random() < 0.04:  # % chance of a 'bad' prediction (-1)
        status = -1
    
    prediction = {
        "value": status
    }
    return prediction

def publish_data():
    """Initializes MQTT client and publishes dummy data."""
    global publisher_client
    publisher_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2) 
    publisher_client.on_connect = on_connect_publisher
    publisher_client.on_message = on_message_publisher # Set the message callback
    publisher_client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    publisher_client.loop_start() # Start the loop in a background thread for receiving messages

    print(f"Publishing dummy data to {TOPIC_LIVE}, {TOPIC_TWIN}, raw, feature, and prediction topics for the motor...")

    try:
        while True:
            # Generate Live Data (still needed for raw/feature/prediction topics for now)
            live_data_payload = generate_live_data()

            # Generate Twin Data - now independent of live_data_payload
            twin_data_payload = generate_twin_data() 

            # Publish Live Data (can be commented out later if raw data comes from external source)
            publisher_client.publish(TOPIC_LIVE, json.dumps(live_data_payload))
            print(f"[MQTT] Sent → Topic: {TOPIC_LIVE} | Payload: {json.dumps(live_data_payload)}")

             # Publish Twin Data 
            publisher_client.publish(TOPIC_TWIN, json.dumps(twin_data_payload))
            print(f"[MQTT] Sent → Topic: {TOPIC_TWIN} | Payload: {json.dumps(twin_data_payload)}")

            # Extract relevant values for Raw Data
            # These values are currently from generate_live_data(), but in a real scenario
            # they would come from actual sensors and be published by a different service.
            raw_temp = live_data_payload.get('temp')
            raw_current = live_data_payload.get('current')
            raw_torque = live_data_payload.get('torque')
            raw_voltage = live_data_payload.get('voltage')
            raw_vibration = live_data_payload.get('vibration')
            raw_rpm = live_data_payload.get('rpm')
            
            # Use a single timestamp for raw, feature, and prediction data to ensure they are the same
            current_timestamp_utc = datetime.datetime.now(utc).isoformat()

            # Publish Raw Data
            # These are the topics the publisher itself subscribes to for its twin data calculation
            if raw_temp is not None:
                raw_temp_payload = {"timestamp": current_timestamp_utc, "value": raw_temp}
                publisher_client.publish(TOPIC_RAW_TEMPERATURE, json.dumps(raw_temp_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_TEMPERATURE} | Payload: {json.dumps(raw_temp_payload)}")
            if raw_current is not None:
                raw_current_payload = {"timestamp": current_timestamp_utc, "value": raw_current}
                publisher_client.publish(TOPIC_RAW_CURRENT, json.dumps(raw_current_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_CURRENT} | Payload: {json.dumps(raw_current_payload)}")
            if raw_torque is not None:
                raw_torque_payload = {"timestamp": current_timestamp_utc, "value": raw_torque}
                publisher_client.publish(TOPIC_RAW_TORQUE, json.dumps(raw_torque_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_TORQUE} | Payload: {json.dumps(raw_torque_payload)}")
            
            if raw_voltage is not None:
                raw_voltage_payload = {"timestamp": current_timestamp_utc, "value": raw_voltage}
                publisher_client.publish(TOPIC_RAW_VOLTAGE, json.dumps(raw_voltage_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_VOLTAGE} | Payload: {json.dumps(raw_voltage_payload)}")
            if raw_vibration is not None:
                raw_vibration_payload = {"timestamp": current_timestamp_utc, "value": int(raw_vibration * 10000)}
                publisher_client.publish(TOPIC_RAW_VIBRATION_VIN, json.dumps(raw_vibration_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_VIBRATION_VIN} | Payload: {json.dumps(raw_vibration_payload)}")
            if raw_rpm is not None:
                raw_rpm_payload = {"timestamp": current_timestamp_utc, "value": raw_rpm}
                publisher_client.publish(TOPIC_RAW_GPIO_RPM, json.dumps(raw_rpm_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_GPIO_RPM} | Payload: {json.dumps(raw_rpm_payload)}")

            # Publish Feature Data (using live data values as a base for feature calculation)
            if raw_temp is not None:
                feature_temp_payload = generate_feature_data_dummy(raw_temp, current_timestamp_utc)
                publisher_client.publish(TOPIC_FEATURE_TEMPERATURE, json.dumps(feature_temp_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_FEATURE_TEMPERATURE} | Payload: {json.dumps(feature_temp_payload)}")
            if raw_current is not None:
                feature_current_payload = generate_feature_data_dummy(raw_current, current_timestamp_utc)
                publisher_client.publish(TOPIC_FEATURE_CURRENT, json.dumps(feature_current_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_FEATURE_CURRENT} | Payload: {json.dumps(feature_current_payload)}")
            if raw_torque is not None:
                feature_torque_payload = generate_feature_data_dummy(raw_torque, current_timestamp_utc)
                publisher_client.publish(TOPIC_FEATURE_TORQUE, json.dumps(feature_torque_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_FEATURE_TORQUE} | Payload: {json.dumps(feature_torque_payload)}")

            # Publish Prediction Data (using live data values as a base for prediction)
            if raw_temp is not None:
                prediction_temp_payload_value = generate_prediction_data_dummy(raw_temp)
                prediction_temp_payload = {"timestamp": current_timestamp_utc, "value": prediction_temp_payload_value["value"]}
                publisher_client.publish(TOPIC_PREDICTION_TEMPERATURE, json.dumps(prediction_temp_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_PREDICTION_TEMPERATURE} | Payload: {json.dumps(prediction_temp_payload)}")
            if raw_current is not None:
                prediction_current_payload_value = generate_prediction_data_dummy(raw_current)
                prediction_current_payload = {"timestamp": current_timestamp_utc, "value": prediction_current_payload_value["value"]}
                publisher_client.publish(TOPIC_PREDICTION_CURRENT, json.dumps(prediction_current_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_PREDICTION_CURRENT} | Payload: {json.dumps(prediction_current_payload)}")
            if raw_torque is not None:
                prediction_torque_payload_value = generate_prediction_data_dummy(raw_torque)
                prediction_torque_payload = {"timestamp": current_timestamp_utc, "value": prediction_torque_payload_value["value"]}
                publisher_client.publish(TOPIC_PREDICTION_TORQUE, json.dumps(prediction_torque_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_PREDICTION_TORQUE} | Payload: {json.dumps(prediction_torque_payload)}")

            time.sleep(5) # Publish every 5 seconds
    except KeyboardInterrupt:
        print("Publisher stopped.")
    finally:
        publisher_client.loop_stop()
        publisher_client.disconnect()

if __name__ == "__main__":
    publish_data()