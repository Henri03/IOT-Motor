import paho.mqtt.client as mqtt
import time
import json
import random
import datetime
import os
import statistics
from pytz import utc # Import UTC timezone

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

# Feature Data Topics
TOPIC_FEATURE_TEMPERATURE = "feature/temperature"
TOPIC_FEATURE_CURRENT = "feature/current"
TOPIC_FEATURE_TORQUE = "feature/torque"

# Prediction Data Topics
TOPIC_PREDICTION_TEMPERATURE = "prediction/temperature"
TOPIC_PREDICTION_CURRENT = "prediction/current"
TOPIC_PREDICTION_TORQUE = "prediction/torque"

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
    }
    return data

def generate_feature_data(metric_value):
    """
    Generates dummy feature data based on a single metric value.
    This simulates a window of data being processed to extract features.
    """
    # Create a dummy window of values around the current metric_value
    # For a more realistic scenario, you'd collect actual historical data
    # and calculate features from that.
    window_size = 10
    # Ensure values are within a reasonable range for the metric
    # For example, temperature features should not be negative
    dummy_data_window = [max(0.0, metric_value + random.uniform(-2, 2)) for _ in range(window_size)]
    
    mean_val = statistics.mean(dummy_data_window)
    min_val = min(dummy_data_window)
    max_val = max(dummy_data_window)
    median_val = statistics.median(dummy_data_window)
    std_dev_val = statistics.stdev(dummy_data_window) if window_size > 1 else 0.0
    range_val = max_val - min_val

    features = {
        "mean": round(mean_val, 3), # Round to 3 decimal places for more precision
        "min": round(min_val, 2),
        "max": round(max_val, 2),
        "median": round(median_val, 2),
        "std": round(std_dev_val, 16), # Standard deviation can be quite precise
        "range": round(range_val, 2),
    }
    return features

def generate_prediction_data(metric_value):
    """
    Generates dummy prediction data based on a single metric value.
    This simulates a model predicting future values or anomaly scores.
    """
    # Simple prediction: a slight deviation from the current value
    predicted_value = round(metric_value * (1 + random.uniform(-0.02, 0.02)), 2)
    
    # Dummy anomaly score (0-1, where 1 is high anomaly)
    anomaly_score = round(random.uniform(0.01, 0.99), 2)
    
    # Dummy prediction of remaining useful life (in hours)
    rul = round(random.uniform(100, 5000), 0)

    # Ensure timestamp is UTC and timezone-aware
    timestamp_utc = datetime.datetime.now(utc).isoformat()

    prediction = {
        "predicted_value": predicted_value,
        "anomaly_score": anomaly_score,
        "rul_hours": rul,
        "timestamp": timestamp_utc,
    }
    return prediction

def on_connect(client, userdata, flags, rc, properties):
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

    print(f"Publishing dummy data to {TOPIC_LIVE}, {TOPIC_TWIN}, raw, feature, and prediction topics for the motor...")

    try:
        while True:
            # Generate Live and Twin Data
            live_data_payload = generate_live_data()
            twin_data_payload = generate_twin_data(live_data_payload)

            # Publish Live Data (UNCOMMENTED)
            client.publish(TOPIC_LIVE, json.dumps(live_data_payload))
            print(f"Published Live Data: {live_data_payload}")

            client.publish(TOPIC_TWIN, json.dumps(twin_data_payload))
            print(f"Published Twin Data: {twin_data_payload}")

            # Extract relevant values for Raw Data
            raw_temp = live_data_payload.get('temp')
            raw_current = live_data_payload.get('current')
            raw_torque = live_data_payload.get('torque')
            
            # Ensure timestamp is UTC and timezone-aware
            current_timestamp_utc = datetime.datetime.now(utc).isoformat()

            # Publish Raw Data
            if raw_temp is not None:
                client.publish(TOPIC_RAW_TEMPERATURE, json.dumps({"timestamp": current_timestamp_utc, "value": raw_temp}))
                print(f"Published Raw Temperature: {raw_temp}")
            if raw_current is not None:
                client.publish(TOPIC_RAW_CURRENT, json.dumps({"timestamp": current_timestamp_utc, "value": raw_current}))
                print(f"Published Raw Current: {raw_current}")
            if raw_torque is not None:
                client.publish(TOPIC_RAW_TORQUE, json.dumps({"timestamp": current_timestamp_utc, "value": raw_torque}))
                print(f"Published Raw Torque: {raw_torque}")

            # Publish Feature Data (using live data values as a base for feature calculation)
            if raw_temp is not None:
                feature_temp_payload = generate_feature_data(raw_temp)
                feature_temp_payload["timestamp"] = current_timestamp_utc 
                client.publish(TOPIC_FEATURE_TEMPERATURE, json.dumps(feature_temp_payload))
                print(f"Published Feature Temperature: {feature_temp_payload}")
            if raw_current is not None:
                feature_current_payload = generate_feature_data(raw_current)
                feature_current_payload["timestamp"] = current_timestamp_utc 
                client.publish(TOPIC_FEATURE_CURRENT, json.dumps(feature_current_payload))
                print(f"Published Feature Current: {feature_current_payload}")
            if raw_torque is not None:
                feature_torque_payload = generate_feature_data(raw_torque)
                feature_torque_payload["timestamp"] = current_timestamp_utc 
                client.publish(TOPIC_FEATURE_TORQUE, json.dumps(feature_torque_payload))
                print(f"Published Feature Torque: {feature_torque_payload}")

            # Publish Prediction Data (using live data values as a base for prediction)
            if raw_temp is not None:
                prediction_temp_payload = generate_prediction_data(raw_temp)
               
                client.publish(TOPIC_PREDICTION_TEMPERATURE, json.dumps(prediction_temp_payload))
                print(f"Published Prediction Temperature: {prediction_temp_payload}")
            if raw_current is not None:
                prediction_current_payload = generate_prediction_data(raw_current)
               
                client.publish(TOPIC_PREDICTION_CURRENT, json.dumps(prediction_current_payload))
                print(f"Published Prediction Current: {prediction_current_payload}")
            if raw_torque is not None:
                prediction_torque_payload = generate_prediction_data(raw_torque)
               
                client.publish(TOPIC_PREDICTION_TORQUE, json.dumps(prediction_torque_payload))
                print(f"Published Prediction Torque: {prediction_torque_payload}")

            time.sleep(5) # Publish every x seconds
    except KeyboardInterrupt:
        print("Publisher stopped.")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    publish_data()