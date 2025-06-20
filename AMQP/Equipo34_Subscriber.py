import pika
import json
import sys
from datetime import datetime
import signal
from collections import deque
import statistics

CLOUDAMQP_URL = "amqps://ekpnsqvv:7-52x28JbCE3ukCG3a2QDLPwhITbYTKT@turkey.rmq.cloudamqp.com/ekpnsqvv"
QUEUE_NAME = "temperature_readings"
HUMIDQ_NAME = "humidity_readings"

TEMPERATURE_ALERT_HIGH = 30.0
TEMPERATURE_ALERT_LOW = 15.0

HUMIDITY_ALERT_HIGH = 55.0
HUMIDITY_ALERT_LOW = 35.0
HISTORY_SIZE = 10  # Number of recent readings to keep for analysis

class HumidityMonitor:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.consuming = False
        self.humidity_history = deque(maxlen=HISTORY_SIZE)
        self.total_readings = 0
        self.alerts_triggered = 0
        self.sensors_seen = set()
        
    def connect_to_cloudamqp(self):
        try:
            url_params = pika.URLParameters(CLOUDAMQP_URL)
            
            self.connection = pika.BlockingConnection(url_params)
            self.channel = self.connection.channel()
            
            self.channel.queue_declare(queue=HUMIDQ_NAME, durable=True)
            
            self.channel.basic_qos(prefetch_count=1)
            
            print(f"Humidity Monitor connected to CloudAMQP successfully!")
            print(f"Monitoring queue: {HUMIDQ_NAME}")
            print(f"Alert thresholds: {HUMIDITY_ALERT_LOW}% - {HUMIDITY_ALERT_HIGH}%")
            return True
            
        except Exception as e:
            print(f"Failed to connect to CloudAMQP: {e}")
            return False
    
    def analyze_humidity(self, humidity):
        alerts = []
        
        if humidity > HUMIDITY_ALERT_HIGH:
            alerts.append(f"HIGH TEMPERATURE ALERT: {humidity}% exceeds {HUMIDITY_ALERT_HIGH}%")
        elif humidity < HUMIDITY_ALERT_LOW:
            alerts.append(f"LOW TEMPERATURE ALERT: {humidity}% below {HUMIDITY_ALERT_LOW}%")
        
        self.humidity_history.append(humidity)
        
        if len(self.humidity_history) >= 3:
            recent_temps = list(self.humidity_history)[-3:]
            hum_change = abs(max(recent_temps) - min(recent_temps))
            
            if hum_change > 5.0:
                alerts.append(f"RAPID HUMIDITY CHANGE: {hum_change:.1f}% variation detected")
        
        return alerts
    
    def get_statistics(self):
        if not self.humidity_history:
            return None
            
        humidities = list(self.humidity_history)
        return {
            "current": humidities[-1],
            "average": round(statistics.mean(humidities), 2),
            "min": min(humidities),
            "max": max(humidities),
            "samples": len(humidities)
        }
    
    def process_humidity_reading(self, ch, method, properties, body):
        try:
            reading = json.loads(body.decode('utf-8'))
            
            sensor_id = reading.get('sensor_id', 'Unknown')
            location = reading.get('location', 'Unknown')
            humidity = reading.get('humidity', 0)
            timestamp = reading.get('timestamp', 'Unknown')
            reading_number = reading.get('reading_number', 0)
            
            self.sensors_seen.add(sensor_id)
            self.total_readings += 1
            
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"\n[{current_time}] Humidity Reading Received:")
            print(f"  Sensor: {sensor_id} | Location: {location}")
            print(f"  humidity: {humidity}%")
            print(f"  Reading #: {reading_number} | Timestamp: {timestamp}")
            
            alerts = self.analyze_humidity(humidity)
            
            for alert in alerts:
                print(f"{alert}")
                self.alerts_triggered += 1
            
            stats = self.get_statistics()
            if stats:
                print(f"  Stats (last {stats['samples']} readings): Avg: {stats['average']}%, Min: {stats['min']}%, Max: {stats['max']}%")
            
            print(f"  Total readings: {self.total_readings} | Active sensors: {len(self.sensors_seen)} | Alerts: {self.alerts_triggered}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except json.JSONDecodeError:
            print(f"Received invalid JSON message: {body.decode('utf-8', errors='ignore')}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error processing humidity reading: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def start_monitoring(self):
        try:
            print(f"\nWaiting for humidity readings from queue '{HUMIDQ_NAME}'")
            print("Press CTRL+C to stop monitoring")
            print("=" * 60)
            
            self.channel.basic_consume(
                queue=HUMIDQ_NAME,
                on_message_callback=self.process_humidity_reading
            )
            
            self.consuming = True
            
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            print("\n\nStopping humidity monitor...")
            self.stop_monitoring()
            
        except Exception as e:
            print(f"Monitor error: {e}")
            self.stop_monitoring()
    
    def stop_monitoring(self):
        if self.consuming and self.channel:
            self.channel.stop_consuming()
            self.consuming = False
            
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Humidity monitor disconnected")
        
        print(f"\nFinal Summary:")
        print(f"  Total readings processed: {self.total_readings}")
        print(f"  Sensors monitored: {len(self.sensors_seen)}")
        print(f"  Alerts triggered: {self.alerts_triggered}")
        if self.sensors_seen:
            print(f"  Sensor IDs: {', '.join(self.sensors_seen)}")
    
    def signal_handler(self, signum, frame):
        print(f"\nReceived signal {signum}, shutting down humidity monitor...")
        self.stop_monitoring()
        sys.exit(0)


class TemperatureMonitor:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.consuming = False
        self.temperature_history = deque(maxlen=HISTORY_SIZE)
        self.total_readings = 0
        self.alerts_triggered = 0
        self.sensors_seen = set()
        
    def connect_to_cloudamqp(self):
        try:
            url_params = pika.URLParameters(CLOUDAMQP_URL)
            
            self.connection = pika.BlockingConnection(url_params)
            self.channel = self.connection.channel()
            
            self.channel.queue_declare(queue=QUEUE_NAME, durable=True)
            
            self.channel.basic_qos(prefetch_count=1)
            
            print(f"Temperature Monitor connected to CloudAMQP successfully!")
            print(f"Monitoring queue: {QUEUE_NAME}")
            print(f"Alert thresholds: {TEMPERATURE_ALERT_LOW}°C - {TEMPERATURE_ALERT_HIGH}°C")
            return True
            
        except Exception as e:
            print(f"Failed to connect to CloudAMQP: {e}")
            return False
    
    def analyze_temperature(self, temperature):
        alerts = []
        
        if temperature > TEMPERATURE_ALERT_HIGH:
            alerts.append(f"HIGH TEMPERATURE ALERT: {temperature}°C exceeds {TEMPERATURE_ALERT_HIGH}°C")
        elif temperature < TEMPERATURE_ALERT_LOW:
            alerts.append(f"LOW TEMPERATURE ALERT: {temperature}°C below {TEMPERATURE_ALERT_LOW}°C")
        
        self.temperature_history.append(temperature)
        
        if len(self.temperature_history) >= 3:
            recent_temps = list(self.temperature_history)[-3:]
            temp_change = abs(max(recent_temps) - min(recent_temps))
            
            if temp_change > 5.0:
                alerts.append(f"RAPID TEMPERATURE CHANGE: {temp_change:.1f}°C variation detected")
        
        return alerts
    
    def get_statistics(self):
        if not self.temperature_history:
            return None
            
        temps = list(self.temperature_history)
        return {
            "current": temps[-1],
            "average": round(statistics.mean(temps), 2),
            "min": min(temps),
            "max": max(temps),
            "samples": len(temps)
        }
    
    def process_temperature_reading(self, ch, method, properties, body):
        try:
            reading = json.loads(body.decode('utf-8'))
            
            sensor_id = reading.get('sensor_id', 'Unknown')
            location = reading.get('location', 'Unknown')
            temperature = reading.get('temperature_celsius', 0)
            timestamp = reading.get('timestamp', 'Unknown')
            reading_number = reading.get('reading_number', 0)
            temp_fahrenheit = reading.get('temperature_fahrenheit', 0)
            
            self.sensors_seen.add(sensor_id)
            self.total_readings += 1
            
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"\n[{current_time}] Temperature Reading Received:")
            print(f"  Sensor: {sensor_id} | Location: {location}")
            print(f"  Temperature: {temperature}°C ({temp_fahrenheit}°F)")
            print(f"  Reading #: {reading_number} | Timestamp: {timestamp}")
            
            alerts = self.analyze_temperature(temperature)
            
            for alert in alerts:
                print(f"{alert}")
                self.alerts_triggered += 1
            
            stats = self.get_statistics()
            if stats:
                print(f"  Stats (last {stats['samples']} readings): Avg: {stats['average']}°C, Min: {stats['min']}°C, Max: {stats['max']}°C")
            
            print(f"  Total readings: {self.total_readings} | Active sensors: {len(self.sensors_seen)} | Alerts: {self.alerts_triggered}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except json.JSONDecodeError:
            print(f"Received invalid JSON message: {body.decode('utf-8', errors='ignore')}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error processing temperature reading: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def start_monitoring(self):
        try:
            print(f"\nWaiting for temperature readings from queue '{QUEUE_NAME}'")
            print("Press CTRL+C to stop monitoring")
            print("=" * 60)
            
            self.channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=self.process_temperature_reading
            )
            
            self.consuming = True
            
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            print("\n\nStopping temperature monitor...")
            self.stop_monitoring()
            
        except Exception as e:
            print(f"Monitor error: {e}")
            self.stop_monitoring()
    
    def stop_monitoring(self):
        if self.consuming and self.channel:
            self.channel.stop_consuming()
            self.consuming = False
            
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Temperature monitor disconnected")
        
        print(f"\nFinal Summary:")
        print(f"  Total readings processed: {self.total_readings}")
        print(f"  Sensors monitored: {len(self.sensors_seen)}")
        print(f"  Alerts triggered: {self.alerts_triggered}")
        if self.sensors_seen:
            print(f"  Sensor IDs: {', '.join(self.sensors_seen)}")
    
    def signal_handler(self, signum, frame):
        print(f"\nReceived signal {signum}, shutting down temperature monitor...")
        self.stop_monitoring()
        sys.exit(0)

def start_monitors(monitorName):
    if monitorName.lower() == "temperature":
        monitor = TemperatureMonitor()
    elif monitorName.lower() == "humidity":
        monitor = HumidityMonitor()
    else:
        return

    signal.signal(signal.SIGINT, monitor.signal_handler)
    signal.signal(signal.SIGTERM, monitor.signal_handler)
    
    if not monitor.connect_to_cloudamqp():
        sys.exit(1)
    
    try:
        monitor.start_monitoring()
        
    except Exception as e:
        print(f"{monitorName} monitor error: {e}")
    
    finally:
        monitor.stop_monitoring()

def main():
    
    monitorName = sys.argv[1] if len(sys.argv) > 1 else "Temperature"
    start_monitors(monitorName)

if __name__ == "__main__":
    main()