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

# Funciones de callback para MQTT
def on_connect(client, userdata, flags, rc):
    print(f"Conectado a MQTT con el código de resultado {rc}")
    for topico in TOPICOS.values():
        client.subscribe(topico)

def on_message(client, userdata, message):
    try:
        data = dict()
        data['valor'] = str(message.payload.decode("utf-8","ignore"))
        tipo_dispositivo = message.topic.split('/')[-1]
        data['tipo_dispositivo'] = tipo_dispositivo
        
        # Lógica específica para cada tópico
        if tipo_dispositivo == 'iluminacion':
            # Suponiendo que 'valor' es el campo en el mensaje para intensidad de iluminación
            data['intensidad'] = data['valor']
        else:
            # Para otros tópicos que solo manejan encendido/apagado (0/1)
            data['estado'] = data['valor']
        
        #print(json.dumps(data))
        collection.insert_one(data)
        #print(f"Datos guardados en MongoDB para {message.topic}")
        
    except Exception as e:
        print(f"Error al procesar mensaje: {e}")

# Cliente MQTT
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Conectar al broker MQTT
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

try:
    while True:
        pass
except KeyboardInterrupt:
    print("Deteniendo el script...")

finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    client.close()
