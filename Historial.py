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
# Tópicos para cada tipo de dispositivo
TOPICOS = {
    "puerta": "jose_univalle/puerta",
    "persianas": "jose_univalle/persianas",
    "iluminacion": "jose_univalle/prueba",
    "ventana": "jose_univalle/ventana"
}

# Cliente MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Función para obtener los últimos registros por tipo de dispositivo
def obtener_ultimos_registros(tipo_dispositivo, limit=5):
    try:
        registros = collection.find({'tipo_dispositivo': tipo_dispositivo}).sort("dateTime", -1).limit(limit)
        return list(registros)
    except Exception as e:
        print(f"Error al obtener datos de MongoDB para {tipo_dispositivo}: {e}")
        return []

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
        for tipo_dispositivo, topico in TOPICOS.items():
            registros = obtener_ultimos_registros(tipo_dispositivo)
            for registro in registros:
                mensaje = json.dumps(registro, default=str)
                mqtt_client.publish(topico, mensaje)
        time.sleep(10)  # Intervalo de tiempo entre cada envío de datos
except KeyboardInterrupt:
    print("Deteniendo el script...")

finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    client.close()
