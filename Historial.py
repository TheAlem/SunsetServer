import pymongo
import paho.mqtt.client as mqtt
import json

# Configuración de MongoDB
MONGO_URI = "mongodb://sunset:1234@144.22.36.59:27017/sunset"
DATABASE_NAME = "sunset"
COLLECTION_NAME = "historial_acceso"

# Configuración de MQTT
MQTT_BROKER = "broker.hivemq.com"  # Reemplaza con la dirección de tu broker
MQTT_PORT = 1883
MQTT_TOPIC = "jose_univalle/puerta"  # Reemplaza con tu tópico

# Cliente MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Función para obtener datos de MongoDB
def obtener_datos():
    # Aquí puedes realizar tu consulta a MongoDB
    documentos = collection.find().limit(1)  # Obtener el último documento como ejemplo
    for doc in documentos:
        return doc
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

# Obtener datos de MongoDB
datos = obtener_datos()
if datos:
    # Publicar datos en el tópico MQTT
    mensaje = json.dumps(datos)  # Convertir el documento a JSON
    mqtt_client.publish(MQTT_TOPIC, mensaje)

# Desconectar el cliente MQTT
mqtt_client.disconnect()

# Cerrar la conexión MongoDB
client.close()
