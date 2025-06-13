import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime

# Configuración de HiveMQ Cloud
HIVEMQ_HOST = "0403acbf0477465fb688063c983d674c.s1.eu.hivemq.cloud"
HIVEMQ_PORT = 8883
HIVEMQ_USERNAME = "hivemq.webclient.1749677139128"
HIVEMQ_PASSWORD = "lT8#r3G;1Ha0zt<kS>UC"

TOPICS = [
    ("sensores/temperatura", 1),
    ("alertas/humedad", 1),
    ("alertas/temperatura", 1)
]

class MQTTSubscriber:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = mqtt.Client()
        self.is_connected = False
        
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        
        self.client.username_pw_set(username, password)
        
        self.client.tls_set()
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado exitosamente a HiveMQ Cloud")
            self.is_connected = True

            for topic, qos in TOPICS:
                client.subscribe(topic, qos)
                print(f"Suscrito al topic: {topic} (QoS: {qos})")
        else:
            print(f"Error de conexión. Código: {rc}")
            self.print_connection_error(rc)
            self.is_connected = False
    
    def on_disconnect(self, client, userdata, rc):
        print("Desconectado de HiveMQ Cloud")
        self.is_connected = False
        
        if rc != 0:
            print("Desconexión inesperada. Intentando reconectar...")
    
    def on_subscribe(self, client, userdata, mid, granted_qos):
        print(f"Suscripción confirmada (ID: {mid}, QoS: {granted_qos})")
    
    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            print("\n" + "="*60)
            print(f"MENSAJE RECIBIDO")
            print(f"Topic: {topic}")
            print(f"QoS: {msg.qos}")
            print(f"Retain: {msg.retain}")
            print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            try:
                json_data = json.loads(payload)
                print(f"Payload (JSON):")
                print(json.dumps(json_data, indent=2, ensure_ascii=False))
                
                if "sensores/temperatura" in topic:
                    self.process_temperature_data(json_data)

                if topic.startswith("alertas"):
                    self.process_alert_data(json_data)

            except json.JSONDecodeError:
                print(f"Payload (Texto): {payload}")
            
            print("="*60)
            
        except Exception as e:
            print(f"Error al procesar mensaje: {e}")
    
    def process_alert_data(self, data):
        """Procesa Alerta"""
        try:
            temp = data.get('temperature')
            humidity = data.get('humidity')
            sensor_id = data.get('sensor_id')
            alerta = data.get('alerta')
            
            if alerta == "Temperatura":
                print(f"Temperatura: {temp}°C")
                
                if temp > 6:
                    print("ALERTA: Temperatura alta!")
                elif temp < 1:
                    print("ALERTA: Temperatura baja!")
            
            elif alerta == "Humedad":
                print(f"Humedad: {humidity}%")
                
                if humidity > 6:
                    print("ALERTA: Humedad alta!")
                elif humidity < 1:
                    print("ALERTA: Humedad baja!")

            if sensor_id:
                print(f"Sensor ID: {sensor_id}")
                
        except Exception as e:
            print(f"Error al procesar datos de temperatura: {e}")
    

    def process_temperature_data(self, data):
        """Procesa datos específicos de sensores de temperatura"""
        try:
            temp = data.get('temperature')
            humidity = data.get('humidity')
            sensor_id = data.get('sensor_id')
            
            if temp is not None:
                print(f"Temperatura: {temp}°C")
             
            if humidity is not None:
                print(f"Humedad: {humidity}%")
            
            if sensor_id:
                print(f"Sensor ID: {sensor_id}")
                
        except Exception as e:
            print(f"Error al procesar datos de temperatura: {e}")
    
    def print_connection_error(self, rc):
        """Imprime descripción del error de conexión"""
        error_messages = {
            1: "Versión de protocolo incorrecta",
            2: "Identificador de cliente inválido",
            3: "Servidor no disponible",
            4: "Nombre de usuario o contraseña incorrectos",
            5: "No autorizado"
        }
        
        if rc in error_messages:
            print(f"Detalle: {error_messages[rc]}")
    
    def connect(self):
        try:
            print(f"Conectando a {self.host}:{self.port}...")
            self.client.connect(self.host, self.port, 60)

            self.client.loop_start()
            
            timeout = 10
            while not self.is_connected and timeout > 0:
                time.sleep(1)
                timeout -= 1
            
            if not self.is_connected:
                raise Exception("Timeout de conexión")
                
        except Exception as e:
            print(f"Error al conectar: {e}")
            return False
        
        return True
    
    def disconnect(self):
        if self.is_connected:
            self.client.loop_stop()
            self.client.disconnect()
            print("Desconectado del broker")

def main():
    subscriber = MQTTSubscriber(HIVEMQ_HOST, HIVEMQ_PORT, HIVEMQ_USERNAME, HIVEMQ_PASSWORD)
    
    if not subscriber.connect():
        return
    
    try:
        print("\nSubscriber activo. Escuchando mensajes...")
        print("Presiona Ctrl+C para detener")
        print("-" * 50)
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nDeteniendo subscriber...")
    
    finally:
        subscriber.disconnect()

if __name__ == "__main__":
    print("Iniciando MQTT Subscriber para HiveMQ Cloud")
    print("Asegúrate de configurar tus credenciales antes de ejecutar")
    print("-" * 50)
    main()