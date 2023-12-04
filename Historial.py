import pymongo
import paho.mqtt.client as mqtt
import json
import time

# Configuración de MongoDB
MONGO_URI = "mongodb://sunset:1234@144.22.36.59:27017/sunset"
DATABASE_NAME = "sunset"
COLLECTION_NAME = "historial_acceso"

# Configuración de MQTT
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "tu/topic/acceso"  # Ajusta este tópico según tus necesidades

# Cliente MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Función para obtener los últimos registros de acceso
def obtener_ultimos_accesos():
    try:
        ultimo_registro = collection.find().sort("dateTime", -1).limit(1)
        return list(ultimo_registro)[0]
    except IndexError:
        return None
    except Exception as e:
        print(f"Error al obtener datos de MongoDB: {e}")
        return None

# Funciones de callback para MQTT
def on_connect(client, userdata, flags, rc):
    print(f"Conectado a MQTT con el código de resultado {rc}")

def on_publish(client, userdata, mid):
    print(f"Mensaje {mid} publicado")

# Cliente MQTT
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_publish = on_publish

# Conectar al broker MQTT
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

try:
    while True:
        # Obtener el último registro de acceso
        datos = obtener_ultimos_accesos()
        if datos:
            # Publicar datos en el tópico MQTT
            mensaje = json.dumps(datos, default=str)  # Convertir a JSON
            mqtt_client.publish(MQTT_TOPIC, mensaje)

        time.sleep(2)  
except KeyboardInterrupt:
    print("Deteniendo el script...")

finally:
    # Desconectar el cliente MQTT y cerrar conexión MongoDB
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    client.close()
