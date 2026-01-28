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
# New topics added as per request
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
    
    # Only check thresholds if status is not already -1 from the random chance.
    # This ensures that -1 predictions remain rare.
    #if status == 1: 
    #    if "temp" in TOPIC_PREDICTION_TEMPERATURE and metric_value > 75: # Higher threshold for bad
     #       status = -1
      #  elif "current" in TOPIC_PREDICTION_CURRENT and metric_value > 22: # Higher threshold for bad
       #     status = -1
        #elif "torque" in TOPIC_PREDICTION_TORQUE and metric_value > 85: # Higher threshold for bad
         #   status = -1

    prediction = {
        "value": status
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

            # Publish Live Data 
            #client.publish(TOPIC_LIVE, json.dumps(live_data_payload))
            #print(f"[MQTT] Sent → Topic: {TOPIC_LIVE} | Payload: {json.dumps(live_data_payload)}")

             # Publish Twin Data 
            client.publish(TOPIC_TWIN, json.dumps(twin_data_payload))
            print(f"[MQTT] Sent → Topic: {TOPIC_TWIN} | Payload: {json.dumps(twin_data_payload)}")

            # Extract relevant values for Raw Data
            raw_temp = live_data_payload.get('temp')
            raw_current = live_data_payload.get('current')
            raw_torque = live_data_payload.get('torque')
            raw_vibration = live_data_payload.get('vibration') # Extract vibration for new topic
            raw_rpm = live_data_payload.get('rpm') # Extract rpm for new topic
            
            # Use a single timestamp for raw, feature, and prediction data to ensure they are the same
            current_timestamp_utc = datetime.datetime.now(utc).isoformat()

            # Publish Raw Data
            if raw_temp is not None:
                raw_temp_payload = {"timestamp": current_timestamp_utc, "value": raw_temp}
                client.publish(TOPIC_RAW_TEMPERATURE, json.dumps(raw_temp_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_TEMPERATURE} | Payload: {json.dumps(raw_temp_payload)}")
            if raw_current is not None:
                raw_current_payload = {"timestamp": current_timestamp_utc, "value": raw_current}
                client.publish(TOPIC_RAW_CURRENT, json.dumps(raw_current_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_CURRENT} | Payload: {json.dumps(raw_current_payload)}")
            if raw_torque is not None:
                raw_torque_payload = {"timestamp": current_timestamp_utc, "value": raw_torque}
                client.publish(TOPIC_RAW_TORQUE, json.dumps(raw_torque_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_TORQUE} | Payload: {json.dumps(raw_torque_payload)}")
            
            # Publish new raw data topics
            if raw_vibration is not None:
                # For Sensor/vin/vibration_raw, value should be an integer
                raw_vibration_payload = {"timestamp": current_timestamp_utc, "value": int(raw_vibration * 10000)} # Example conversion to integer
                client.publish(TOPIC_RAW_VIBRATION_VIN, json.dumps(raw_vibration_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_VIBRATION_VIN} | Payload: {json.dumps(raw_vibration_payload)}")
            if raw_rpm is not None:
                raw_rpm_payload = {"timestamp": current_timestamp_utc, "value": raw_rpm}
                client.publish(TOPIC_RAW_GPIO_RPM, json.dumps(raw_rpm_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_RAW_GPIO_RPM} | Payload: {json.dumps(raw_rpm_payload)}")

            # Publish Feature Data (using live data values as a base for feature calculation)
            if raw_temp is not None:
                feature_temp_payload = generate_feature_data_dummy(raw_temp, current_timestamp_utc) # Pass timestamp
                client.publish(TOPIC_FEATURE_TEMPERATURE, json.dumps(feature_temp_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_FEATURE_TEMPERATURE} | Payload: {json.dumps(feature_temp_payload)}")
            if raw_current is not None:
                feature_current_payload = generate_feature_data_dummy(raw_current, current_timestamp_utc) # Pass timestamp
                client.publish(TOPIC_FEATURE_CURRENT, json.dumps(feature_current_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_FEATURE_CURRENT} | Payload: {json.dumps(feature_current_payload)}")
            if raw_torque is not None:
                feature_torque_payload = generate_feature_data_dummy(raw_torque, current_timestamp_utc) # Pass timestamp
                client.publish(TOPIC_FEATURE_TORQUE, json.dumps(feature_torque_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_FEATURE_TORQUE} | Payload: {json.dumps(feature_torque_payload)}")

            # Publish Prediction Data (using live data values as a base for prediction)
            # The timestamp for prediction must be the same as for raw data
            if raw_temp is not None:
                prediction_temp_payload_value = generate_prediction_data_dummy(raw_temp)
                prediction_temp_payload = {"timestamp": current_timestamp_utc, "value": prediction_temp_payload_value["value"]}
                client.publish(TOPIC_PREDICTION_TEMPERATURE, json.dumps(prediction_temp_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_PREDICTION_TEMPERATURE} | Payload: {json.dumps(prediction_temp_payload)}")
            if raw_current is not None:
                prediction_current_payload_value = generate_prediction_data_dummy(raw_current)
                prediction_current_payload = {"timestamp": current_timestamp_utc, "value": prediction_current_payload_value["value"]}
                client.publish(TOPIC_PREDICTION_CURRENT, json.dumps(prediction_current_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_PREDICTION_CURRENT} | Payload: {json.dumps(prediction_current_payload)}")
            if raw_torque is not None:
                prediction_torque_payload_value = generate_prediction_data_dummy(raw_torque)
                prediction_torque_payload = {"timestamp": current_timestamp_utc, "value": prediction_torque_payload_value["value"]}
                client.publish(TOPIC_PREDICTION_TORQUE, json.dumps(prediction_torque_payload))
                print(f"[MQTT] Sent → Topic: {TOPIC_PREDICTION_TORQUE} | Payload: {json.dumps(prediction_torque_payload)}")

            time.sleep(5) # Publish every 5 seconds
    except KeyboardInterrupt:
        print("Publisher stopped.")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    publish_data()