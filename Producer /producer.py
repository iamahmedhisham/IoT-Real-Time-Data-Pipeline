import time
import uuid
import json
import random
from datetime import datetime
import copy 

# MQTT & TLS
import ssl
import paho.mqtt.client as mqtt

# --- OpenMeteo setup ---
import openmeteo_requests
import requests_cache
from retry_requests import retry

# --- AWS IoT Core MQTT Setup ---
device_id = "farm_data"
cert_path = "farm_certe/certificate.pem.crt"
key_path = "farm_certe/private.pem.key"
ca_path = "farm_certe/AmazonRootCA1.pem"
endpoint = "a100dwshfdyv3m-ats.iot.us-east-1.amazonaws.com"
port = 8883
topic = "farm/data"

# --- Init MQTT Client ---
client = mqtt.Client(client_id=device_id)
client.tls_set(
    ca_certs=ca_path,
    certfile=cert_path,
    keyfile=key_path,
    tls_version=ssl.PROTOCOL_TLSv1_2
)
client.connect(endpoint, port)
client.loop_start()

# --- OpenMeteo API setup ---
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# --- Define 3 Locations - Updated to match Lambda expectations ---
locations = [
    {"loc_id": "loc_1", "lat": 23.4219, "lon": 30.5978, "name": "Toshka_project"},
    {"loc_id": "loc_2", "lat": 22.4214, "lon": 28.5306, "name": "Sharq_El_Owainat_project"},
    {"loc_id": "loc_3", "lat": 30.6558, "lon": 30.5401, "name": "Dina_Farms"}
]

# Updated sensor ranges to match Lambda validation ranges exactly
sensor_ranges = {
    "loc_1": {
        "temperature": {"min": 10.0, "max": 50.0, "optimal": (18.0, 28.0)},
        "humidity": {"min": 30.0, "max": 90.0, "optimal": (45.0, 75.0)},
        "water_level": {"min": 0.5, "max": 3.0, "optimal": (1.2, 2.2)},
        "nitrogen": {"min": 80.0, "max": 150.0, "optimal": (100.0, 130.0)},
        "phosphorus": {"min": 40.0, "max": 80.0, "optimal": (50.0, 70.0)},
        "potassium": {"min": 40.0, "max": 80.0, "optimal": (50.0, 70.0)},
        "ph": {"min": 6.0, "max": 8.0, "optimal": (6.5, 7.2)}
    },
    "loc_2": {
        "temperature": {"min": 15.0, "max": 55.0, "optimal": (22.0, 32.0)},
        "humidity": {"min": 25.0, "max": 80.0, "optimal": (40.0, 65.0)},
        "water_level": {"min": 0.3, "max": 2.5, "optimal": (1.0, 2.0)},
        "nitrogen": {"min": 70.0, "max": 140.0, "optimal": (90.0, 120.0)},
        "phosphorus": {"min": 30.0, "max": 70.0, "optimal": (40.0, 60.0)},
        "potassium": {"min": 30.0, "max": 70.0, "optimal": (40.0, 60.0)},
        "ph": {"min": 6.5, "max": 8.5, "optimal": (7.0, 7.8)}
    },
    "loc_3": {
        "temperature": {"min": 12.0, "max": 52.0, "optimal": (20.0, 30.0)},
        "humidity": {"min": 28.0, "max": 85.0, "optimal": (42.0, 70.0)},
        "water_level": {"min": 0.4, "max": 2.8, "optimal": (1.1, 2.3)},
        "nitrogen": {"min": 75.0, "max": 145.0, "optimal": (95.0, 125.0)},
        "phosphorus": {"min": 35.0, "max": 75.0, "optimal": (45.0, 65.0)},
        "potassium": {"min": 35.0, "max": 75.0, "optimal": (45.0, 65.0)},
        "ph": {"min": 6.2, "max": 8.2, "optimal": (6.8, 7.5)}
    }
}

# --- Enhanced Fault Logic with Realistic Progression ---
# Global state tracking
record_count = 0
system_health_degradation = 0.0  # Tracks overall system degradation over time
fault_history = []  # Track fault patterns for realistic progression
last_sensor_values = {}

# Realistic fault configuration
STABLE_PERIOD_RECORDS = 10  # First 1000 records are mostly stable
GRADUAL_DEGRADATION_START = 8  # Start very subtle issues around record 800
FAULT_ESCALATION_RATE = 0.1  # How quickly faults increase after stable period

def calculate_fault_probabilities():
    """Calculate realistic fault probabilities based on system lifecycle"""
    global record_count, system_health_degradation
    
    if record_count < GRADUAL_DEGRADATION_START:
        # Early period - very stable system
        return 0.001, 0.002  # Almost no faults
    
    elif record_count < STABLE_PERIOD_RECORDS:
        # Late stable period - minor wear showing
        return 0.005, 0.01   # Very low fault rates
    
    else:
        # Post-stable period - gradual degradation
        excess_records = record_count - STABLE_PERIOD_RECORDS
        
        # Exponential degradation curve (realistic for equipment)
        base_invalid_rate = 0.02
        base_alert_rate = 0.03
        
        # System degradation increases over time
        degradation_factor = min(1.0 + (excess_records * FAULT_ESCALATION_RATE / 100), 3.0)
        
        invalid_rate = min(base_invalid_rate * degradation_factor, 0.15)  # Cap at 15%
        alert_rate = min(base_alert_rate * degradation_factor, 0.25)     # Cap at 25%
        
        return invalid_rate, alert_rate

def inject_realistic_major_fault(record, loc_id):
    """Inject realistic major faults based on actual sensor failure patterns"""
    
    # More realistic fault patterns based on system age
    if record_count < STABLE_PERIOD_RECORDS + 100:
        fault_types = ["sensor_drift", "communication_glitch"]  # Early issues
    elif record_count < STABLE_PERIOD_RECORDS + 500:
        fault_types = ["sensor_freeze", "power_fluctuation", "calibration_error"]  # Mid-stage
    else:
        fault_types = ["sensor_failure", "extreme_values", "hardware_degradation"]  # Late stage
    
    fault_type = random.choice(fault_types)
    
    if fault_type == "sensor_drift":
        print(f"-> Injecting REALISTIC fault: Sensor drift in {loc_id}")
        # Gradual sensor drift - values slowly go out of range
        drift_sensors = random.sample(list(record["sensor_data"].keys()), random.randint(1, 2))
        for sensor in drift_sensors:
            ranges = sensor_ranges[loc_id][sensor]
            # Drift beyond normal range but not extremely
            if random.choice([True, False]):
                record["sensor_data"][sensor] = ranges["max"] * random.uniform(1.1, 1.3)
            else:
                record["sensor_data"][sensor] = ranges["min"] * random.uniform(0.7, 0.9)
    
    elif fault_type == "communication_glitch":
        print(f"-> Injecting REALISTIC fault: Communication glitch in {loc_id}")
        # Missing data due to communication issues
        keys_to_potentially_remove = ["sensor_data", "weather_data"]
        if random.random() < 0.7:  # 70% chance to corrupt sensor_data
            record["sensor_data"] = {}
        else:
            del record[random.choice(keys_to_potentially_remove)]
    
    elif fault_type == "sensor_freeze":
        print(f"-> Injecting REALISTIC fault: Sensor freeze in {loc_id}")
        # Sensor stuck at last value or zero
        freeze_value = random.choice([0, 9999, last_sensor_values.get(loc_id, {}).get(random.choice(list(record["sensor_data"].keys())), 0)])
        sensors_to_freeze = random.sample(list(record["sensor_data"].keys()), random.randint(1, 2))
        for sensor in sensors_to_freeze:
            record["sensor_data"][sensor] = freeze_value
    
    elif fault_type == "power_fluctuation":
        print(f"-> Injecting REALISTIC fault: Power fluctuation in {loc_id}")
        # Power issues cause erratic readings
        for sensor in record["sensor_data"]:
            if random.random() < 0.3:  # 30% chance each sensor affected
                record["sensor_data"][sensor] = random.choice([0, -9999, 'NULL', None])
    
    elif fault_type == "calibration_error":
        print(f"-> Injecting REALISTIC fault: Calibration error in {loc_id}")
        # Systematic error in readings
        error_factor = random.uniform(1.5, 3.0)  # 50-200% error
        affected_sensors = random.sample(list(record["sensor_data"].keys()), random.randint(2, 4))
        for sensor in affected_sensors:
            current_val = record["sensor_data"][sensor]
            if isinstance(current_val, (int, float)):
                record["sensor_data"][sensor] = current_val * error_factor
    
    elif fault_type == "sensor_failure":
        print(f"-> Injecting REALISTIC fault: Complete sensor failure in {loc_id}")
        # Complete sensor failure
        failed_sensors = random.sample(list(record["sensor_data"].keys()), random.randint(1, 3))
        for sensor in failed_sensors:
            record["sensor_data"][sensor] = random.choice([9999, -9999, 'FAIL', None])
    
    elif fault_type == "hardware_degradation":
        print(f"-> Injecting REALISTIC fault: Hardware degradation in {loc_id}")
        # Multiple sensors showing age-related issues
        for sensor in record["sensor_data"]:
            if random.random() < 0.4:  # 40% chance each sensor degraded
                record["sensor_data"][sensor] = random.choice([0, 'NaN', -9999])

    return record

def inject_realistic_alert_fault(record, loc_id):
    """Inject realistic conditions that should trigger alerts"""
    
    # Choose realistic scenarios based on environmental factors
    current_hour = datetime.now().hour
    
    # More likely scenarios based on time and conditions
    if 12 <= current_hour <= 16:  # Afternoon heat
        scenarios = ["high_temperature", "low_water_level", "high_evaporation"]
        weights = [0.5, 0.3, 0.2]
    elif 0 <= current_hour <= 6:   # Night/early morning
        scenarios = ["low_temperature", "ph_imbalance", "condensation_issues"]
        weights = [0.4, 0.4, 0.2]
    else:  # Normal hours
        scenarios = ["nutrient_depletion", "ph_imbalance", "irrigation_issues"]
        weights = [0.4, 0.3, 0.3]
    
    scenario = random.choices(scenarios, weights=weights)[0]
    ranges = sensor_ranges[loc_id]
    
    if scenario == "high_temperature":
        print(f"-> Injecting REALISTIC alert: Heat stress in {loc_id}")
        # Realistic high temperature that would stress crops
        record["sensor_data"]["temperature"] = random.uniform(36.0, 42.0)
        # Corresponding effects
        record["sensor_data"]["humidity"] = max(25.0, record["sensor_data"]["humidity"] * 0.8)
        record["sensor_data"]["water_level"] = max(0.5, record["sensor_data"]["water_level"] * 0.9)
    
    elif scenario == "low_temperature":
        print(f"-> Injecting REALISTIC alert: Cold stress in {loc_id}")
        record["sensor_data"]["temperature"] = random.uniform(2.0, 8.0)
        # Plants might struggle with nutrient uptake in cold
        record["sensor_data"]["nitrogen"] = record["sensor_data"]["nitrogen"] * 0.9
    
    elif scenario == "low_water_level":
        print(f"-> Injecting REALISTIC alert: Drought conditions in {loc_id}")
        record["sensor_data"]["water_level"] = random.uniform(0.3, 0.8)
        # Nutrient concentration might increase due to less water
        for nutrient in ["nitrogen", "phosphorus", "potassium"]:
            record["sensor_data"][nutrient] = min(ranges[nutrient]["max"], 
                                                record["sensor_data"][nutrient] * 1.2)
    
    elif scenario == "ph_imbalance":
        print(f"-> Injecting REALISTIC alert: Soil pH imbalance in {loc_id}")
        if random.choice([True, False]):
            record["sensor_data"]["ph"] = random.uniform(5.0, 5.8)  # Too acidic
            print(f"   -> Acidic soil (pH: {record['sensor_data']['ph']:.2f})")
        else:
            record["sensor_data"]["ph"] = random.uniform(7.8, 8.5)  # Too alkaline
            print(f"   -> Alkaline soil (pH: {record['sensor_data']['ph']:.2f})")
    
    elif scenario == "nutrient_depletion":
        nutrients = ["nitrogen", "phosphorus", "potassium"]
        depleted_nutrient = random.choice(nutrients)
        print(f"-> Injecting REALISTIC alert: {depleted_nutrient} depletion in {loc_id}")
        min_val = ranges[depleted_nutrient]["min"]
        record["sensor_data"][depleted_nutrient] = min_val * random.uniform(0.6, 0.85)
    
    elif scenario == "irrigation_issues":
        print(f"-> Injecting REALISTIC alert: Irrigation system issues in {loc_id}")
        record["sensor_data"]["water_level"] = random.uniform(0.4, 0.9)
        record["sensor_data"]["humidity"] = max(25.0, record["sensor_data"]["humidity"] * 0.7)
    
    elif scenario == "high_evaporation":
        print(f"-> Injecting REALISTIC alert: High evaporation rate in {loc_id}")
        record["sensor_data"]["water_level"] = random.uniform(0.5, 1.0)
        record["sensor_data"]["humidity"] = min(95.0, record["sensor_data"]["humidity"] * 1.3)
    
    elif scenario == "condensation_issues":
        print(f"-> Injecting REALISTIC alert: Excessive humidity in {loc_id}")
        record["sensor_data"]["humidity"] = random.uniform(85.0, 95.0)
        # High humidity might affect temperature readings
        record["sensor_data"]["temperature"] = record["sensor_data"]["temperature"] * 0.95
        
    return record

def generate_optimal_sensor_data(loc_id):
    """Generate sensor data within optimal ranges for healthy operation"""
    ranges = sensor_ranges[loc_id]
    sensor_data = {}
    
    for sensor, config in ranges.items():
        optimal_min, optimal_max = config["optimal"]
        
        # Generate values mostly in optimal range with some natural variation
        if sensor in last_sensor_values.get(loc_id, {}):
            # Smooth transition from last value
            last_val = last_sensor_values[loc_id][sensor]
            
            # Small natural variation (±2% of optimal range)
            variation_range = (optimal_max - optimal_min) * 0.02
            new_val = last_val + random.uniform(-variation_range, variation_range)
            
            # Keep within optimal bounds (with 90% probability) or allow slight drift
            if random.random() < 0.9:
                new_val = max(optimal_min, min(optimal_max, new_val))
            else:
                # Allow occasional drift outside optimal but within valid range
                new_val = max(config["min"], min(config["max"], new_val))
        else:
            # Initial value in optimal range
            new_val = random.uniform(optimal_min, optimal_max)
        
        sensor_data[sensor] = round(new_val, 2)
    
    return sensor_data

def fetch_weather(latitude, longitude):
    """Fetch weather data from OpenMeteo API"""
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": [
                "temperature_2m", "relative_humidity_2m", "is_day",
                "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
                "rain", "precipitation", "surface_pressure", "apparent_temperature"
            ]
        }
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        current = response.Current()
        
        return {
            "temperature_2m": current.Variables(0).Value(),
            "relative_humidity_2m": current.Variables(1).Value(),
            "is_day": current.Variables(2).Value(),
            "wind_speed_10m": current.Variables(3).Value(),
            "wind_direction_10m": current.Variables(4).Value(),
            "wind_gusts_10m": current.Variables(5).Value(),
            "rain": current.Variables(6).Value(),
            "precipitation": current.Variables(7).Value(),
            "surface_pressure": current.Variables(8).Value(),
            "apparent_temperature": current.Variables(9).Value()
        }
    except Exception as e:
        print(f"Weather API error: {e}")
        # Return dummy weather data if API fails
        return {
            "temperature_2m": random.uniform(20, 35),
            "relative_humidity_2m": random.uniform(40, 80),
            "is_day": 1,
            "wind_speed_10m": random.uniform(0, 15),
            "wind_direction_10m": random.uniform(0, 360),
            "wind_gusts_10m": random.uniform(0, 20),
            "rain": 0,
            "precipitation": 0,
            "surface_pressure": random.uniform(1000, 1020),
            "apparent_temperature": random.uniform(18, 38)
        }

def format_api_record(api_data, loc_id, latitude, longitude):
    """Create a properly formatted record with realistic fault progression"""
    global record_count
    
    # Generate optimal sensor data as baseline
    sensor_data = generate_optimal_sensor_data(loc_id)
    
    # Create the base record
    record = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "loc_id": loc_id,
        "location": {"latitude": latitude, "longitude": longitude},
        "sensor_data": sensor_data,
        "weather_data": api_data
    }
    
    # Calculate current fault probabilities
    invalid_prob, alert_prob = calculate_fault_probabilities()
    
    # Apply realistic fault injection
    fault_applied = False
    if random.random() < invalid_prob:
        record = inject_realistic_major_fault(copy.deepcopy(record), loc_id)
        fault_applied = True
        print(f"🔴 Generated INVALID record for {loc_id} (prob: {invalid_prob:.3f})")
    elif random.random() < alert_prob:
        record = inject_realistic_alert_fault(copy.deepcopy(record), loc_id)
        fault_applied = True
        print(f"🟡 Generated ALERT-triggering record for {loc_id} (prob: {alert_prob:.3f})")
    else:
        print(f"🟢 Generated HEALTHY record for {loc_id}")

    # Update sensor history only for non-corrupted records
    if not fault_applied and "sensor_data" in record and record["sensor_data"]:
        if loc_id not in last_sensor_values:
            last_sensor_values[loc_id] = {}
        last_sensor_values[loc_id].update(record["sensor_data"])
    
    return record

def print_system_status():
    """Print current system health status"""
    invalid_prob, alert_prob = calculate_fault_probabilities()
    
    if record_count < GRADUAL_DEGRADATION_START:
        status = "🟢 EXCELLENT"
    elif record_count < STABLE_PERIOD_RECORDS:
        status = "🟡 GOOD"
    elif record_count < STABLE_PERIOD_RECORDS + 200:
        status = "🟠 DEGRADING"
    else:
        status = "🔴 POOR"
    
    print(f"\n{'='*60}")
    print(f"📊 SYSTEM STATUS: {status}")
    print(f"📈 Total Records: {record_count}")
    print(f"🔧 Invalid Fault Rate: {invalid_prob:.1%}")
    print(f"⚠️  Alert Trigger Rate: {alert_prob:.1%}")
    print(f"{'='*60}")

# --- Main Loop ---
print("🚀 Starting Enhanced Farm Data Producer with Realistic Fault Progression...")
print(f"📍 Locations: {[loc['loc_id'] for loc in locations]}")
print(f"⏱️  Stable period: First {STABLE_PERIOD_RECORDS} records")
print(f"📉 Gradual degradation starts at record {GRADUAL_DEGRADATION_START}")

try:
    while True:
        for loc in locations:
            record_count += 1
            
            # Print system status every 50 records
            if record_count % 50 == 0:
                print_system_status()
            
            print(f"\n--- Record #{record_count} for {loc['loc_id']} ({loc['name']}) ---")
            
            # Fetch weather data
            weather_data = fetch_weather(loc["lat"], loc["lon"])
            
            # Create record with realistic fault progression
            record = format_api_record(weather_data, loc["loc_id"], loc["lat"], loc["lon"])
            
            # Add some debug info to see sensor values
            if record.get("sensor_data"):
                temp = record["sensor_data"].get("temperature", "N/A")
                water = record["sensor_data"].get("water_level", "N/A")
                ph = record["sensor_data"].get("ph", "N/A")
                print(f"   Temp: {temp}°C, Water: {water}m, pH: {ph}")
            
            # Publish to MQTT
            result = client.publish(topic, json.dumps(record))
            if result.rc != 0:
                print(f"❌ Failed to publish message, return code: {result.rc}")
            else:
                print(f"✅ Published to topic `{topic}`")
            
            # Save to local file for debugging
            with open("realistic_farm_data.jsonl", "a") as f:
                f.write(json.dumps(record) + "\n")
        
        print(f"\n{'='*50}")
        print(f"⏳ Batch completed. Waiting 3 seconds...")
        print(f"{'='*50}")
        time.sleep(3)
        
except KeyboardInterrupt:
    print("\n🛑 Shutting down producer...")
    print_system_status()
except Exception as e:
    print(f"❌ Error in main loop: {e}")
    time.sleep(5)

# Cleanup
client.loop_stop()
client.disconnect()
print("🏁 Producer stopped.")
