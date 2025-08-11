import boto3
import json
import uuid
import base64
import random
from datetime import datetime, timedelta

# --- 1. Configuration ---
# S3 configuration
s3_client = boto3.client('s3')
S3_PROCESSED_BUCKET = "farm-processed-data"
VALID_PREFIX = "valid/"
INVALID_PREFIX = "invalid/"

# SNS configuration
sns_client = boto3.client('sns')
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:311141522446:farm_aletrs"

# Alert throttling config
ALERT_INTERVAL = timedelta(minutes=5)
CONSECUTIVE_RECORDS_THRESHOLD = 1

# Global alert state (in production, use DynamoDB or ElastiCache)
alert_state = {
    "last_sent": {},
    "consecutive_counts": {}
}

# Updated to match producer exactly
EXPECTED_RANGES_PER_LOCATION = {
    "loc_1": {
        "temperature": (10, 50),
        "humidity": (30, 90),
        "water_level": (0.5, 3.0),
        "nitrogen": (80, 150),
        "phosphorus": (40, 80),
        "potassium": (40, 80),
        "ph": (6.0, 8.0)
    },
    "loc_2": {
        "temperature": (15, 55),
        "humidity": (25, 80),
        "water_level": (0.3, 2.5),
        "nitrogen": (70, 140),
        "phosphorus": (30, 70),
        "potassium": (30, 70),
        "ph": (6.5, 8.5)
    },
    "loc_3": {
        "temperature": (12, 52),
        "humidity": (28, 85),
        "water_level": (0.4, 2.8),
        "nitrogen": (75, 145),
        "phosphorus": (35, 75),
        "potassium": (35, 75),
        "ph": (6.2, 8.2)
    }
}

def validate_record(data):
    """
    Comprehensive record validation with detailed error reporting
    """
    errors = []
    warnings = []
    
    print(f"Validating record: {data.get('event_id', 'unknown')}")
    
    # Check location ID
    loc_id = data.get("loc_id")
    if not loc_id:
        errors.append("missing_loc_id")
        return "INVALID", errors, warnings
    
    if loc_id not in EXPECTED_RANGES_PER_LOCATION:
        errors.append(f"invalid_loc_id:{loc_id}")
        return "INVALID", errors, warnings
    
    location_ranges = EXPECTED_RANGES_PER_LOCATION[loc_id]
    
    # Check required top-level keys
    required_keys = ["event_id", "timestamp", "sensor_data", "weather_data", "location"]
    for key in required_keys:
        if key not in data:
            errors.append(f"missing_top_level_key:{key}")

    # If sensor_data is missing, can't continue validation
    sensor_data = data.get("sensor_data", {})
    if not sensor_data:
        errors.append("missing_sensor_data")
        return "INVALID", errors, warnings

    # Validate each sensor
    for sensor_name, (min_val, max_val) in location_ranges.items():
        val = sensor_data.get(sensor_name)
        
        # Check for extreme/sentinel values that indicate sensor failure
        extreme_values = [0, 9999, -9999, '0', '9999', '-9999', 'null', 'NULL', 'NaN', None]
        if val in extreme_values:
            errors.append(f"sensor_data:{sensor_name}_extreme_value")
            continue
        
        # Check for missing sensor data
        if val is None:
            errors.append(f"sensor_data:{sensor_name}_missing")
            continue
        
        # Validate data type and convert if possible
        if not isinstance(val, (int, float)):
            try:
                converted_val = float(val)
                sensor_data[sensor_name] = converted_val
                val = converted_val
                warnings.append(f"sensor_data:{sensor_name}_type_converted")
            except (ValueError, TypeError):
                errors.append(f"sensor_data:{sensor_name}_invalid_type")
                continue
        
        # Check if value is within expected range
        if not (min_val <= val <= max_val):
            # Check if it's close to threshold (within 10% buffer)
            buffer = (max_val - min_val) * 0.1
            if (min_val - buffer) <= val <= (max_val + buffer):
                warnings.append(f"sensor_data:{sensor_name}_near_threshold")
            else:
                errors.append(f"sensor_data:{sensor_name}_out_of_range")

    # Cross-validate with weather data
    if "temperature" in sensor_data and "weather_data" in data:
        weather_data = data["weather_data"]
        if "temperature_2m" in weather_data:
            sensor_temp = sensor_data["temperature"]
            weather_temp = weather_data["temperature_2m"]
            
            # Allow reasonable difference between sensor and weather
            if abs(sensor_temp - weather_temp) > 15:
                warnings.append(f"temperature_mismatch:{sensor_temp}vs{weather_temp}")

    # Determine overall status
    if errors:
        status = "INVALID"
        print(f"Record INVALID: {len(errors)} errors, {len(warnings)} warnings")
    elif warnings:
        status = "WARNING"
        print(f"Record WARNING: {len(warnings)} warnings")
    else:
        status = "VALID"
        print(f"Record VALID")
    
    return status, errors, warnings

def handle_alerts(record, status, errors=[], warnings=[]):
    """
    Generate alerts based on record status and sensor values
    """
    alerts = []
    loc_id = record.get("loc_id", "unknown")
    sensor_data = record.get("sensor_data", {})
    
    print(f"Generating alerts for {loc_id}, status: {status}")
    
    # Critical alerts for invalid data
    if status == "INVALID":
        sensor_errors = [e for e in errors if e.startswith("sensor_data")]
        
        if sensor_errors:
            # Group errors by type
            error_types = set()
            for error in sensor_errors:
                if "extreme_value" in error:
                    error_types.add("sensor_failure")
                elif "missing" in error:
                    error_types.add("sensor_disconnected")
                elif "out_of_range" in error:
                    error_types.add("sensor_malfunction")
            
            for error_type in error_types:
                alerts.append({
                    "type": "Sensor Failure",
                    "priority": "CRITICAL",
                    "description": f"Critical sensor issue detected at {loc_id}: {error_type}"
                })

    # Operational alerts for valid data
    if status in ["VALID", "WARNING"]:
        # Temperature alerts
        temp = sensor_data.get("temperature")
        if temp is not None and isinstance(temp, (int, float)):
            if temp > 35:
                alerts.append({
                    "type": "High Temperature",
                    "priority": "HIGH",
                    "description": f"High temperature warning: {temp:.1f}°C at {loc_id}"
                })
            elif temp < 5:
                alerts.append({
                    "type": "Low Temperature",
                    "priority": "HIGH", 
                    "description": f"Low temperature warning: {temp:.1f}°C at {loc_id}"
                })
        
        # Water level alerts
        water_level = sensor_data.get("water_level")
        if water_level is not None and isinstance(water_level, (int, float)):
            if water_level < 1.0:
                alerts.append({
                    "type": "Low Water Level",
                    "priority": "HIGH",
                    "description": f"Low water level alert: {water_level:.2f}m at {loc_id}"
                })
            elif water_level > 2.5:
                alerts.append({
                    "type": "High Water Level",
                    "priority": "MEDIUM",
                    "description": f"High water level: {water_level:.2f}m at {loc_id}"
                })
        
        # Soil pH alerts
        ph_level = sensor_data.get("ph")
        if ph_level is not None and isinstance(ph_level, (int, float)):
            if ph_level < 6.0 or ph_level > 7.5:
                priority = "HIGH" if ph_level < 5.5 or ph_level > 8.0 else "MEDIUM"
                alerts.append({
                    "type": "Soil pH Warning",
                    "priority": priority,
                    "description": f"Soil pH out of optimal range: {ph_level:.1f} at {loc_id}"
                })
        
        # Nutrient alerts
        for nutrient in ["nitrogen", "phosphorus", "potassium"]:
            value = sensor_data.get(nutrient)
            if value is not None and isinstance(value, (int, float)):
                ranges = EXPECTED_RANGES_PER_LOCATION[loc_id]
                min_val, max_val = ranges[nutrient]
                
                if value < min_val * 0.8:  # 20% below minimum
                    alerts.append({
                        "type": "Low Nutrient",
                        "priority": "MEDIUM",
                        "description": f"Low {nutrient} level: {value:.1f} at {loc_id}"
                    })

    print(f"Generated {len(alerts)} alerts for {loc_id}")
    return alerts

def check_and_send_alert(alert, loc_id, event_id, timestamp):
    """
    Send alert with throttling and confirmation logic
    """
    now = datetime.utcnow()
    alert_type = alert["type"]
    alert_key = f"{loc_id}|{alert_type}"
    
    print(f"Processing alert: {alert_key}, Priority: {alert['priority']}")
    
    # Critical alerts bypass throttling
    if alert.get("priority") == "CRITICAL":
        print(f"Critical alert - sending immediately: {alert_key}")
        send_alert(alert, loc_id, event_id, timestamp)
        alert_state["last_sent"][alert_key] = now
        return True
    
    # Check time-based throttling
    last_sent = alert_state["last_sent"].get(alert_key)
    if last_sent:
        time_since_last = now - last_sent
        if time_since_last < ALERT_INTERVAL:
            print(f"Alert throttled: {alert_key} (last sent {time_since_last} ago)")
            return False
    
    # Check consecutive records threshold
    count = alert_state["consecutive_counts"].get(alert_key, 0) + 1
    alert_state["consecutive_counts"][alert_key] = count
    
    if count >= CONSECUTIVE_RECORDS_THRESHOLD:
        send_alert(alert, loc_id, event_id, timestamp)
        alert_state["last_sent"][alert_key] = now
        alert_state["consecutive_counts"][alert_key] = 0
        print(f"Alert sent after {count} consecutive occurrences: {alert_key}")
        return True
    else:
        print(f"Alert queued ({count}/{CONSECUTIVE_RECORDS_THRESHOLD}): {alert_key}")
        return False

def send_alert(alert, loc_id, event_id, timestamp):
    """
    Send alert via SNS
    """
    subject = f"🚨 {alert['priority']} Alert: {alert['type']} @ {loc_id}"
    
    message = (
        f"🚨 Farm IoT Alert Notification\n\n"
        f"📍 Location: {loc_id}\n"
        f"🕒 Timestamp: {timestamp}\n"
        f"⚠️  Alert Type: {alert['type']}\n"
        f"📊 Priority: {alert['priority']}\n"
        f"📝 Description: {alert['description']}\n\n"
        f"🔧 Recommended Action: {get_recommended_action(alert['type'])}\n\n"
        f"🆔 Event ID: {event_id}\n"
        f"🤖 Generated by Farm Monitoring System"
    )
    
    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print(f"✅ Alert sent successfully: {subject}")
        print(f"SNS Message ID: {response.get('MessageId', 'unknown')}")
        return True
    except Exception as e:
        print(f"❌ Failed to send SNS alert: {str(e)}")
        return False

def get_recommended_action(alert_type):
    """
    Return recommended action based on alert type
    """
    actions = {
        "High Temperature": "Increase irrigation frequency and check cooling systems",
        "Low Temperature": "Check heating systems and frost protection",
        "Low Water Level": "Inspect irrigation system and water supply",
        "High Water Level": "Check drainage systems and reduce irrigation",
        "Soil pH Warning": "Test soil samples and adjust pH levels as needed",
        "Low Nutrient": "Schedule fertilizer application and soil testing",
        "Sensor Failure": "Immediate sensor inspection and replacement required"
    }
    
    return actions.get(alert_type, "Investigate the issue and contact technical support")


def flatten_record(record, parent_key='', sep='_'):

    flattened = {}
    for key, value in record.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        
        if isinstance(value, dict):

            flattened.update(flatten_record(value, new_key, sep))
        elif isinstance(value, list):

            flattened[new_key] = json.dumps(value)
        else:
            flattened[new_key] = value
    
    return flattened


def upload_to_s3(data, prefix):
    """
    Upload processed data to S3 with metadata
    """
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        loc_id = data.get("loc_id", "unknown")
        event_id = data.get("event_id", f"event_{uuid.uuid4().hex[:8]}")
        
        flat_data = flatten_record(data)
        flat_data["processing_timestamp"] = datetime.utcnow().isoformat() + "Z"
        flat_data["processor_version"] = "1.0"

        # Create structured S3 key
        #date_path = datetime.utcnow().strftime("%Y/%m/%d")
        #s3_key = f"{prefix}{date_path}/{loc_id}/{timestamp}_{event_id}.json"
        s3_key = f"{prefix}{loc_id}/{timestamp}_{event_id}.json"
        
        # Add processing metadata
        data["processing_timestamp"] = datetime.utcnow().isoformat() + "Z"
        data["processor_version"] = "1.0"
        
        metadata = {
            'location': loc_id,
            'event_time': data.get("timestamp", ""),
            'status': data.get("validation_status", "UNKNOWN"),
            'processor': 'farm-data-processor-v1'
        }
        
        s3_client.put_object(
            Bucket=S3_PROCESSED_BUCKET,
            Key=s3_key,
            Body=json.dumps(flat_data, indent=2),
            ContentType="application/json",
            Metadata=metadata
        )
        
        print(f"📁 Uploaded to S3: s3://{S3_PROCESSED_BUCKET}/{s3_key}")
        return s3_key
        
    except Exception as e:
        print(f"❌ S3 upload failed: {str(e)}")
        return None

def lambda_handler(event, context):
    """
    Main Lambda handler for processing Kinesis records
    """
    print(f"Processing {len(event['Records'])} records")
    
    # Periodic alert state cleanup to prevent memory buildup
    if random.random() < 0.1:  # 10% chance
        # Clean up old alert states (older than 1 hour)
        current_time = datetime.utcnow()
        old_keys = []
        
        for key, last_time in alert_state["last_sent"].items():
            if isinstance(last_time, datetime) and (current_time - last_time).total_seconds() > 3600:
                old_keys.append(key)
        
        for key in old_keys:
            alert_state["last_sent"].pop(key, None)
            alert_state["consecutive_counts"].pop(key, None)
        
        if old_keys:
            print(f"🧹 Cleaned up {len(old_keys)} old alert states")
    
    processed_count = 0
    error_count = 0
    alert_count = 0
    
    for record in event["Records"]:
        payload = None
        try:
            # Decode Kinesis data
            encoded_data = record["kinesis"]["data"]
            payload = base64.b64decode(encoded_data).decode("utf-8")
            data = json.loads(payload)
            
            print(f"\n--- Processing record {processed_count + 1} ---")
            print(f"Event ID: {data.get('event_id', 'unknown')}")
            print(f"Location: {data.get('loc_id', 'unknown')}")
            
            # Validate the record
            status, errors, warnings = validate_record(data)
            
            # Add validation results to record
            data["validation_status"] = status
            data["validation_errors"] = errors
            data["validation_warnings"] = warnings
            data["validation_timestamp"] = datetime.utcnow().isoformat() + "Z"
            
            print(f"Validation result: {status}")
            if errors:
                print(f"Errors: {errors}")
            if warnings:
                print(f"Warnings: {warnings}")
            
            # Generate and process alerts
            alerts = handle_alerts(data, status, errors, warnings)
            
            if alerts:
                data["alerts"] = alerts
                alert_count += len(alerts)
                
                # Process each alert
                for alert in alerts:
                    loc_id = data.get("loc_id", "unknown")
                    event_id = data.get("event_id", "unknown")
                    timestamp = data.get("timestamp", datetime.utcnow().isoformat())
                    
                    alert_sent = check_and_send_alert(alert, loc_id, event_id, timestamp)
                    if alert_sent:
                        # Mark that an alert was sent for this record
                        if "alerts_sent" not in data:
                            data["alerts_sent"] = []
                        data["alerts_sent"].append({
                            "type": alert["type"],
                            "priority": alert["priority"],
                            "sent_timestamp": datetime.utcnow().isoformat() + "Z"
                        })
            
            # Upload to appropriate S3 location based on status
            if status == "VALID":
                s3_key = upload_to_s3(data, VALID_PREFIX)
            elif status == "WARNING":
                s3_key = upload_to_s3(data, VALID_PREFIX + "warnings/")
            else:  # INVALID
                s3_key = upload_to_s3(data, INVALID_PREFIX)
            
            # Add S3 location to record for reference
            if s3_key:
                data["s3_location"] = f"s3://{S3_PROCESSED_BUCKET}/{s3_key}"
            
            processed_count += 1
            print(f"✅ Record processed successfully")
            
        except json.JSONDecodeError as e:
            error_count += 1
            print(f"❌ JSON decode error: {str(e)}")
            
            # Save malformed record for debugging
            try:
                error_data = {
                    "error_type": "json_decode_error",
                    "error_message": str(e),
                    "raw_payload": payload or "Unable to decode base64",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "kinesis_record_id": record.get("kinesis", {}).get("sequenceNumber", "unknown")
                }
                upload_to_s3(error_data, "errors/json_decode/")
            except Exception as inner_e:
                print(f"Failed to save JSON decode error: {str(inner_e)}")
                
        except Exception as e:
            error_count += 1
            print(f"❌ Unexpected error processing record: {str(e)}")
            
            # Save error record for debugging
            try:
                error_data = {
                    "error_type": "processing_error",
                    "error_message": str(e),
                    "raw_payload": payload,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "kinesis_record_id": record.get("kinesis", {}).get("sequenceNumber", "unknown")
                }
                
                # Try to extract basic info from payload if possible
                if payload:
                    try:
                        partial_data = json.loads(payload)
                        error_data["event_id"] = partial_data.get("event_id")
                        error_data["loc_id"] = partial_data.get("loc_id")
                    except:
                        pass
                
                upload_to_s3(error_data, "errors/processing/")
            except Exception as inner_e:
                print(f"Failed to save processing error: {str(inner_e)}")
    
    # Prepare response
    response = {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Batch processing completed",
            "processed_records": processed_count,
            "error_records": error_count,
            "total_alerts_generated": alert_count,
            "processing_timestamp": datetime.utcnow().isoformat() + "Z"
        })
    }
    
    print(f"\n🏁 Batch Summary:")
    print(f"   Processed: {processed_count}")
    print(f"   Errors: {error_count}")
    print(f"   Alerts: {alert_count}")
    
    return response
