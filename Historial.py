import pymongo
import paho.mqtt.client as mqtt
import time
from datetime import datetime


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

# Diccionario para almacenar el último mensaje de cada tópico
ultimo_mensaje = {topico: {"mensaje": None, "timestamp": 0} for topico in TOPICOS.values()}

# Tiempo mínimo entre mensajes (en segundos)
INTERVALO_MINIMO = 2

# Funciones de callback para MQTT
def on_connect(client, userdata, flags, rc):
    print(f"Conectado a MQTT con el código de resultado {rc}")
    for topico in TOPICOS.values():
        client.subscribe(topico)

def on_message(client, userdata, message):
    mensaje_actual = message.payload.decode("utf-8","ignore")
    topico = message.topic
    tiempo_actual = time.time()

    # Verificar si el mensaje es diferente del último o si ha pasado suficiente tiempo
    if mensaje_actual != ultimo_mensaje[topico]["mensaje"] or \
        (tiempo_actual - ultimo_mensaje[topico]["timestamp"]) > INTERVALO_MINIMO:

        try:
            data = {
                'valor': mensaje_actual,
                'tipo_dispositivo': topico.split('/')[-1],
                'fecha_hora': datetime.now()  # Añadir fecha y hora actual
            }

            # Lógica específica para cada tópico
            if data['tipo_dispositivo'] == 'iluminacion':
                data['intensidad'] = data['valor']
            else:
                data['estado'] = data['valor']
            
            collection.insert_one(data)

            # Actualizar el último mensaje y timestamp
            ultimo_mensaje[topico] = {"mensaje": mensaje_actual, "timestamp": tiempo_actual}
            
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
