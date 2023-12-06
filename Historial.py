import json
import time
import pymongo
import paho.mqtt.client as mqtt
from threading import Thread, Event

# Configuración de MongoDB
MONGO_URI = "mongodb://sunset:1234@144.22.36.59:27017/sunset"
DATABASE_NAME = "sunset"
COLLECTION_NAME = "historial_acceso"

# Configuración de MQTT
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
TOPICOS_ENTRADA = ["jose_univalle/puerta", "jose_univalle/persianas", "jose_univalle/iluminacion", "jose_univalle/ventana"]

# Cliente MongoDB
client_mongo = pymongo.MongoClient(MONGO_URI)
db = client_mongo[DATABASE_NAME]
collection = db[COLLECTION_NAME]


# Cliente MQTT
client_mqtt = mqtt.Client()

# Evento para controlar la ejecución de los hilos
stop_event = Event()


# Funciones de callback para MQTT
def on_connect(client, rc):
    print(f"Conectado a MQTT con el código de resultado {rc}")
    for topico in TOPICOS_ENTRADA:
        client.subscribe(topico)

def on_message(message):
    print(f"Mensaje recibido del tópico {message.topic}")
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

# Configuración de callbacks MQTT
client_mqtt.on_connect = on_connect
client_mqtt.on_message = on_message

# Iniciar el bucle MQTT en un hilo separado
def iniciar_mqtt():
    client_mqtt.connect(MQTT_BROKER, MQTT_PORT, 60)
    while not stop_event.is_set():
        client_mqtt.loop(timeout=1.0)  # Ejecutar loop con un timeout

# Iniciar hilo MQTT
thread_mqtt = Thread(target=iniciar_mqtt)
thread_mqtt.start()

# Bucle principal
try:
    while True:

        time.sleep(10)

except KeyboardInterrupt:
    print("Deteniendo el script...")

finally:
    stop_event.set()  # Indicar a los hilos que se detengan
    thread_mqtt.join()  # Esperar a que el hilo MQTT termine
    client_mqtt.disconnect()
    client_mongo.close()
