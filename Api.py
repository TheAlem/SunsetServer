from fastapi import FastAPI
from pymongo import MongoClient
from typing import Optional

app = FastAPI()

# Configuración de MongoDB
MONGO_URI = "mongodb://sunset:1234@144.22.36.59:27017/sunset"
DATABASE_NAME = "sunset"
COLLECTION_NAME = "historial_acceso"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

@app.get("/historial/{tipo_dispositivo}")
def read_historial(tipo_dispositivo: str):
    try:
        # Filtrar los datos por tipo de dispositivo
        historial_data = list(collection.find({"tipo_dispositivo": tipo_dispositivo}, {"_id": 0}))
        return {"success": True, "data": historial_data}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
