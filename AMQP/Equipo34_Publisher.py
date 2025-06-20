import pika
import json
import sys
import time
import random
from datetime import datetime
import signal
import math


CLOUDAMQP_URL = "amqps://ekpnsqvv:7-52x28JbCE3ukCG3a2QDLPwhITbYTKT@turkey.rmq.cloudamqp.com/ekpnsqvv"
QUEUE_NAME = "temperature_readings"
HUMIDITY_QNAME = "humidity_readings"

SENSOR_ID = "TEMP_SENSOR_001"
HUMIDITY_SID = "HUM_SENSOR_001"
BASE_HUMIDITY = 45.0
HUMIDITY_VARIANCE = 5.0

BASE_TEMPERATURE = 22.5
TEMPERATURE_VARIANCE = 5.0

READING_INTERVAL = 2
LOCATION = "Server Room A"

class HumiditySensor:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.running = False
        self.reading_count = 0
        
    def connect_to_cloudamqp(self):
        try:
            url_params = pika.URLParameters(CLOUDAMQP_URL)
            
            self.connection = pika.BlockingConnection(url_params)
            self.channel = self.connection.channel()
            
            self.channel.queue_declare(queue=HUMIDITY_QNAME, durable=True)
            
            print(f"Humidity sensor connected to CloudAMQP successfully!")
            print(f"Sensor ID: {SENSOR_ID}")
            print(f"Location: {LOCATION}")
            print(f"Reading interval: {READING_INTERVAL} seconds")
            return True
            
        except Exception as e:
            print(f"Failed to connect to CloudAMQP: {e}")
            return False
    
    def generate_humidity_reading(self):
        humidity_offset = random.uniform(-HUMIDITY_VARIANCE, HUMIDITY_VARIANCE)
        humidity = round(BASE_HUMIDITY + humidity_offset, 2)
        
        time_drift = math.sin(time.time() / 100) * 2
        humidity += time_drift
        humidity = round(humidity, 2)
        
        return humidity
    
    def create_sensor_message(self, humidity):
        self.reading_count += 1
        
        message = {
            "sensor_id": HUMIDITY_SID,
            "location": LOCATION,
            "humidity": humidity,
            "timestamp": datetime.now().isoformat(),
            "reading_number": self.reading_count,
            "sensor_status": "active",
            "unit": "%"
        }
        
        return message
    
    def publish_humidity_reading(self):
        try:
            humidity = self.generate_humidity_reading()
            
            message = self.create_sensor_message(humidity)
            
            self.channel.basic_publish(
                exchange='',
                routing_key=HUMIDITY_QNAME,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    headers={'sensor_type': 'temperature', 'location': LOCATION}
                )
            )
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Reading #{self.reading_count}: {humidity}%")
            return True
            
        except Exception as e:
            print(f"Failed to publish humidity reading: {e}")
            return False
    
    def start_sensor(self):
        print("\nStarting humidity sensor...")
        print("Press Ctrl+C to stop the sensor")
        print("=" * 50)
        
        self.running = True
        
        try:
            while self.running:
                if not self.publish_humidity_reading():
                    print("Failed to publish reading, retrying in 5 seconds...")
                    time.sleep(5)
                    continue
            
                time.sleep(READING_INTERVAL)
                
        except KeyboardInterrupt:
            print("\n\nStopping humidity sensor...")
            self.stop_sensor()
        except Exception as e:
            print(f"Sensor error: {e}")
            self.stop_sensor()
    
    def stop_sensor(self):
        self.running = False
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Sensor disconnected")
    
    def signal_handler(self, signum, frame):
        print(f"\nReceived signal {signum}, shutting down sensor...")
        self.stop_sensor()
        sys.exit(0)


class TemperatureSensor:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.running = False
        self.reading_count = 0
        
    def connect_to_cloudamqp(self):
        try:
            url_params = pika.URLParameters(CLOUDAMQP_URL)
            
            self.connection = pika.BlockingConnection(url_params)
            self.channel = self.connection.channel()
            
            self.channel.queue_declare(queue=QUEUE_NAME, durable=True)
            
            print(f"Temperature sensor connected to CloudAMQP successfully!")
            print(f"Sensor ID: {SENSOR_ID}")
            print(f"Location: {LOCATION}")
            print(f"Reading interval: {READING_INTERVAL} seconds")
            return True
            
        except Exception as e:
            print(f"Failed to connect to CloudAMQP: {e}")
            return False
    
    def generate_temperature_reading(self):
        temperature_offset = random.uniform(-TEMPERATURE_VARIANCE, TEMPERATURE_VARIANCE)
        temperature = round(BASE_TEMPERATURE + temperature_offset, 2)
        
        time_drift = math.sin(time.time() / 100) * 2
        temperature += time_drift
        temperature = round(temperature, 2)
        
        return temperature
    
    def create_sensor_message(self, temperature):
        self.reading_count += 1
        
        message = {
            "sensor_id": SENSOR_ID,
            "location": LOCATION,
            "temperature_celsius": temperature,
            "temperature_fahrenheit": round((temperature * 9/5) + 32, 2),
            "timestamp": datetime.now().isoformat(),
            "reading_number": self.reading_count,
            "sensor_status": "active",
            "unit": "celsius"
        }
        
        return message
    
    def publish_temperature_reading(self):
        try:
            temperature = self.generate_temperature_reading()
            
            message = self.create_sensor_message(temperature)
            
            self.channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    headers={'sensor_type': 'temperature', 'location': LOCATION}
                )
            )
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Reading #{self.reading_count}: {temperature}°C ({message['temperature_fahrenheit']}°F)")
            return True
            
        except Exception as e:
            print(f"Failed to publish temperature reading: {e}")
            return False
    
    def start_sensor(self):
        print("\nStarting temperature sensor...")
        print("Press Ctrl+C to stop the sensor")
        print("=" * 50)
        
        self.running = True
        
        try:
            while self.running:
                if not self.publish_temperature_reading():
                    print("Failed to publish reading, retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                
                time.sleep(READING_INTERVAL)
                
        except KeyboardInterrupt:
            print("\n\nStopping temperature sensor...")
            self.stop_sensor()
        except Exception as e:
            print(f"Sensor error: {e}")
            self.stop_sensor()
    
    def stop_sensor(self):
        self.running = False
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Sensor disconnected")
    
    def signal_handler(self, signum, frame):
        print(f"\nReceived signal {signum}, shutting down sensor...")
        self.stop_sensor()
        sys.exit(0)

def start_sensors(sensorName):
    if sensorName.lower() == "humidity":
        sensor = HumiditySensor()
    
    elif sensorName.lower() == "temperature":
        sensor = TemperatureSensor()

    else:
        return

    signal.signal(signal.SIGINT, sensor.signal_handler)
    signal.signal(signal.SIGTERM, sensor.signal_handler)

    if not sensor.connect_to_cloudamqp():
        sys.exit(1)
    
    try:
        sensor.start_sensor()
        
    except Exception as e:
        print(f" {sensorName} Sensor error: {e}")
    
    finally:
        sensor.stop_sensor()

def main():
    sensorName = sys.argv[1] if len(sys.argv) > 1 else "Temperature"

    start_sensors(sensorName)

if __name__ == "__main__":
    main()