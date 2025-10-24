"""Script para verificar la configuración de MongoDB"""
from pymongo import MongoClient

# CAMBIA ESTOS VALORES SEGÚN TU CONFIGURACIÓN
HOST = 'localhost'
PORT = 27017
DB_NAME = 'airbnb_cdmx'  # ← CAMBIA ESTO

try:
    client = MongoClient(HOST, PORT, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print(f"✓ Conectado a MongoDB en {HOST}:{PORT}")
    
    # Listar bases de datos
    print("\nBases de datos disponibles:")
    for db in client.list_database_names():
        print(f"  - {db}")
    
    # Verificar tu base de datos
    db = client[DB_NAME]
    print(f"\nColecciones en '{DB_NAME}':")
    for col in db.list_collection_names():
        count = db[col].count_documents({})
        print(f"  - {col}: {count} documentos")
    
    # Verificar columnas de listings
    print("\nColumnas en 'listings' (primeros 5):")
    sample = db['listings'].find_one()
    if sample:
        for key in list(sample.keys())[:10]:
            print(f"  - {key}")
    
    client.close()
    print("\n✓ Todo correcto")
    
except Exception as e:
    print(f"✗ Error: {e}")
    print("\nVerifica:")
    print("1. MongoDB está corriendo: sudo systemctl status mongod")
    print("2. El nombre de la base de datos es correcto")
    print("3. Las colecciones existen")
