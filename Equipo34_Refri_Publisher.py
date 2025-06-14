import paho.mqtt.client as mqtt
import json
import time
import random
from datetime import datetime

# Conexión HiveMQ
HIVEMQ_HOST = "0403acbf0477465fb688063c983d674c.s1.eu.hivemq.cloud"
HIVEMQ_PORT = 8883
HIVEMQ_USERNAME = "hivemq.webclient.1749677139128"
HIVEMQ_PASSWORD = "lT8#r3G;1Ha0zt<kS>UC"

MACHINE_NAME = "GTenorio"

# Topicos
TOPIC_TEMP = "sensores/temperatura"
TOPIC_HUMID = "sensores/humedad"
TOPIC_PING = "sensores/health"
ALERT_TEMP = "alertas/temperatura"
ALERT_HUMID = "alertas/humedad"

class MQTTPublisher:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = mqtt.Client()
        self.is_connected = False
        
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        
        self.client.username_pw_set(username, password)
        
        self.client.tls_set()
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado exitosamente a HiveMQ Cloud")
            self.is_connected = True
        else:
            print(f"Error de conexión. Código: {rc}")
            self.is_connected = False
    
    def on_disconnect(self, client, userdata, rc):
        print("Desconectado de HiveMQ Cloud")
        self.is_connected = False
    
    def on_publish(self, client, userdata, mid):
        print(f"Mensaje publicado (ID: {mid})")
    
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
    
    def publish_message(self, topic, message):
        if not self.is_connected:
            print("No hay conexión disponible")
            return False
        
        try:
            if isinstance(message, dict):
                message = json.dumps(message)
            
            result = self.client.publish(topic, message, qos=1)
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"Mensaje enviado al topic '{topic}': {message}")
                return True
            else:
                print(f"Error al publicar mensaje: {result.rc}")
                return False
                
        except Exception as e:
            print(f"Error al publicar: {e}")
            return False
    
    def disconnect(self):
        if self.is_connected:
            self.client.loop_stop()
            self.client.disconnect()
            print("Desconectado del broker")

def main():
    publisher = MQTTPublisher(HIVEMQ_HOST, HIVEMQ_PORT, HIVEMQ_USERNAME, HIVEMQ_PASSWORD)
    
    if not publisher.connect():
        return
    
    try:
        counter = 1
        
        while True:
            sensor = counter % 5
            almacen = counter % 3
            message = {
                "timestamp": datetime.now().isoformat(),
                "sensor_id": f"Refrigerador: {sensor}",
                "location": f"Almacen {almacen}",
                "origin": MACHINE_NAME
            }

            

            if (MACHINE_NAME == "Lucia"):
                message["temperature"] = random.randint(-5, 10)
                publisher.publish_message(TOPIC_TEMP, message)

            elif (MACHINE_NAME == "German"):
                message["humidity"] = random.randint(20, 65)
                publisher.publish_message(TOPIC_HUMID, message)

            elif(MACHINE_NAME == "GTenorio"):
                publisher.publish_message(TOPIC_PING, message)

                message["temperature"] = random.randint(-5, 10)
                message["humidity"] = random.randint(20, 65)

                if(message["temperature"] > 4 or message["temperature"] < 1):
                    message["alerta"] = "Temperatura"
                    publisher.publish_message(ALERT_TEMP, message)
                
                if(message["humidity"] > 55 or message["humidity"] < 30):
                    message["alerta"] = "Humedad"
                    publisher.publish_message(ALERT_HUMID, message)
            
            counter += 1
            print("Esperando 5 segundos...")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nDeteniendo publisher...")
    
    finally:
        publisher.disconnect()

if __name__ == "__main__":
    print("Iniciando MQTT Publisher para HiveMQ Cloud")
    print("Asegúrate de configurar tus credenciales antes de ejecutar")
    print("-" * 50)
    main()