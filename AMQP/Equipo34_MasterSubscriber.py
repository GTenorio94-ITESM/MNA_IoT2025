import pika
import json
import sys
import threading
import time
import signal
from datetime import datetime
from collections import defaultdict, deque

CLOUDAMQP_URL = "amqps://ekpnsqvv:7-52x28JbCE3ukCG3a2QDLPwhITbYTKT@turkey.rmq.cloudamqp.com/ekpnsqvv"

QUEUE_CONFIGS = [
    {
        "name": "temperature_readings",
        "processor": "temperature",
        "description": "Temperature sensor data",
        "color": "\033[91m"
    },
    {
        "name": "humidity_readings", 
        "processor": "humidity",
        "description": "Humidity sensor data",
        "color": "\033[94m"
    }
]

COLORS = {
    'RESET': '\033[0m',
    'BOLD': '\033[1m',
    'RED': '\033[91m',
    'GREEN': '\033[92m',
    'YELLOW': '\033[93m',
    'BLUE': '\033[94m',
    'MAGENTA': '\033[95m',
    'CYAN': '\033[96m'
}

class QueueSubscriber:
    
    def __init__(self, queue_config, stats_collector):
        self.queue_name = queue_config["name"]
        self.processor_type = queue_config["processor"]
        self.description = queue_config["description"]
        self.color = queue_config["color"]
        self.stats_collector = stats_collector
        
        self.connection = None
        self.channel = None
        self.consuming = False
        self.thread = None
        self.should_stop = threading.Event()
        
    def connect(self):
        try:
            url_params = pika.URLParameters(CLOUDAMQP_URL)
            self.connection = pika.BlockingConnection(url_params)
            self.channel = self.connection.channel()
            
            # Declare queue
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1)
            
            return True
        except Exception as e:
            print(f"{self.color}[{self.queue_name}] Connection failed: {e}{COLORS['RESET']}")
            return False
    
    def process_message(self, ch, method, properties, body):
        try:
            try:
                message_data = json.loads(body.decode('utf-8'))
            except json.JSONDecodeError:
                message_data = {"raw_message": body.decode('utf-8', errors='ignore')}
            
            self.stats_collector.update_stats(self.queue_name, message_data)
            
            if self.processor_type == "temperature":
                self.process_temperature(message_data)
            elif self.processor_type == "humidity":
                self.process_humidity(message_data)
            else:
                return
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"{self.color}[{self.queue_name}] Processing error: {e}{COLORS['RESET']}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def process_temperature(self, data):
        temp_c = data.get('temperature_celsius', 'N/A')
        temp_f = data.get('temperature_fahrenheit', 'N/A')
        sensor_id = data.get('sensor_id', 'Unknown')
        location = data.get('location', 'Unknown')
        
        alert = ""
        if isinstance(temp_c, (int, float)):
            if temp_c > 30:
                alert = " [HIGH TEMP ALERT]"
            elif temp_c < 15:
                alert = " [LOW TEMP ALERT]"
        
        print(f"{self.color}[TEMP] {sensor_id} @ {location}: {temp_c}°C ({temp_f}°F){alert}{COLORS['RESET']}")
    
    def process_humidity(self, data):
        humidity = data.get('humidity_percent', data.get('humidity', 'N/A'))
        sensor_id = data.get('sensor_id', 'Unknown')
        location = data.get('location', 'Unknown')
        
        alert = ""
        if isinstance(humidity, (int, float)):
            if humidity > 80:
                alert = " [HIGH HUMIDITY]"
            elif humidity < 30:
                alert = " [LOW HUMIDITY]"
        
        print(f"{self.color}[HUMID] {sensor_id} @ {location}: {humidity}%{alert}{COLORS['RESET']}")

    def start_consuming(self):
        """Start consuming messages in a separate thread"""
        self.thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.thread.start()
        print(f"{self.color}[{self.queue_name}] Started subscriber thread{COLORS['RESET']}")
    
    def _consume_loop(self):
        try:
            if not self.connect():
                return
            
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.process_message
            )
            
            self.consuming = True
            print(f"{self.color}[{self.queue_name}] Waiting for messages...{COLORS['RESET']}")
            
            while not self.should_stop.is_set():
                try:
                    self.connection.process_data_events(time_limit=1)
                except Exception as e:
                    if not self.should_stop.is_set():
                        print(f"{self.color}[{self.queue_name}] Connection error: {e}{COLORS['RESET']}")
                        break
            
        except Exception as e:
            print(f"{self.color}[{self.queue_name}] Consumer error: {e}{COLORS['RESET']}")
        finally:
            self.stop()
    
    def stop(self):
        self.should_stop.set()
        self.consuming = False
        
        if self.channel and not self.channel.is_closed:
            try:
                self.channel.stop_consuming()
            except:
                pass
        
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except:
                pass
        
        print(f"{self.color}[{self.queue_name}] Subscriber stopped{COLORS['RESET']}")

class StatsCollector:
    
    def __init__(self):
        self.message_counts = defaultdict(int)
        self.recent_messages = defaultdict(lambda: deque(maxlen=5))
        self.start_time = datetime.now()
        self.lock = threading.Lock()
    
    def update_stats(self, queue_name, message_data):
        with self.lock:
            self.message_counts[queue_name] += 1
            self.recent_messages[queue_name].append({
                'timestamp': datetime.now(),
                'data': message_data
            })
    
    def print_stats(self):
        with self.lock:
            print(f"\n{COLORS['BOLD']}{COLORS['CYAN']}=== MULTI-QUEUE STATISTICS ==={COLORS['RESET']}")
            print(f"Running time: {datetime.now() - self.start_time}")
            print(f"Total queues: {len(self.message_counts)}")
            
            for queue_name, count in self.message_counts.items():
                recent_count = len(self.recent_messages[queue_name])
                print(f"  {queue_name}: {count} total messages ({recent_count} recent)")
            
            total_messages = sum(self.message_counts.values())
            print(f"Total messages processed: {total_messages}")
            print(f"{COLORS['CYAN']}================================{COLORS['RESET']}\n")

class MultiQueueSubscriber:
    
    def __init__(self):
        self.subscribers = []
        self.stats_collector = StatsCollector()
        self.running = False
        self.stats_thread = None
    
    def setup_subscribers(self):
        print(f"{COLORS['BOLD']}Setting up subscribers for {len(QUEUE_CONFIGS)} queues...{COLORS['RESET']}")
        
        for queue_config in QUEUE_CONFIGS:
            subscriber = QueueSubscriber(queue_config, self.stats_collector)
            self.subscribers.append(subscriber)
            print(f"  - {queue_config['name']}: {queue_config['description']}")
        
        print()
    
    def start_all_subscribers(self):
        print(f"{COLORS['BOLD']}Starting all subscribers...{COLORS['RESET']}")
        
        for subscriber in self.subscribers:
            subscriber.start_consuming()
            time.sleep(0.1)
        
        self.stats_thread = threading.Thread(target=self._stats_loop, daemon=True)
        self.stats_thread.start()
        
        self.running = True
        print(f"{COLORS['GREEN']}All subscribers started successfully!{COLORS['RESET']}")
        print(f"{COLORS['YELLOW']}Press Ctrl+C to stop all subscribers{COLORS['RESET']}")
        print("=" * 60)
    
    def _stats_loop(self):
        while self.running:
            time.sleep(30)
            if self.running:
                self.stats_collector.print_stats()
    
    def stop_all_subscribers(self):
        """Stop all subscribers"""
        print(f"\n{COLORS['YELLOW']}Stopping all subscribers...{COLORS['RESET']}")
        self.running = False
        
        for subscriber in self.subscribers:
            subscriber.stop()
        
        for subscriber in self.subscribers:
            if subscriber.thread and subscriber.thread.is_alive():
                subscriber.thread.join(timeout=2)
        
        self.stats_collector.print_stats()
        print(f"{COLORS['GREEN']}All subscribers stopped successfully!{COLORS['RESET']}")
    
    def signal_handler(self, signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        self.stop_all_subscribers()
        sys.exit(0)

def main():
    print(f"{COLORS['BOLD']}{COLORS['CYAN']}CloudAMQP Multi-Queue Subscriber{COLORS['RESET']}")
    print(f"{COLORS['CYAN']}===================================={COLORS['RESET']}")
    
    multi_subscriber = MultiQueueSubscriber()
    
    signal.signal(signal.SIGINT, multi_subscriber.signal_handler)
    signal.signal(signal.SIGTERM, multi_subscriber.signal_handler)
    
    try:
        multi_subscriber.setup_subscribers()
        multi_subscriber.start_all_subscribers()

        while multi_subscriber.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        multi_subscriber.stop_all_subscribers()
    except Exception as e:
        print(f"{COLORS['RED']}Multi-subscriber error: {e}{COLORS['RESET']}")
        multi_subscriber.stop_all_subscribers()

if __name__ == "__main__":
    main()