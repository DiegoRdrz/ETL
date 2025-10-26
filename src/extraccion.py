import logging
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os


class Extraccion:
    """Extrae datos de MongoDB y los carga en DataFrames"""
    
    def __init__(self, host='localhost', port=27017, db_name='Airbnb_mx'):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None
        self._setup_log()
    
    def _setup_log(self):
        """Configura el log"""
        log_dir = os.path.join(os.path.dirname(__file__), '../logs')
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f'extraccion_{timestamp}.log')

        # Logger independiente
        self.logger = logging.getLogger(f"extraccion_{timestamp}")
        self.logger.setLevel(logging.INFO)

        # Evita duplicar handlers si se instancia más de una vez
        if not self.logger.handlers:
            fh = logging.FileHandler(log_file)
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)
    
    def conectar(self):
        """Conecta a MongoDB"""
        try:
            self.client = MongoClient(self.host, self.port)
            self.db = self.client[self.db_name]
            self.logger.info(f"Conectado a {self.db_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error conexión: {e}")
            return False
    
    def extraer_coleccion(self, nombre):
        """Extrae una colección y retorna DataFrame"""
        try:
            datos = list(self.db[nombre].find())
            df = pd.DataFrame(datos)
            self.logger.info(f"{nombre}: {len(df)} registros")
            return df
        except Exception as e:
            self.logger.error(f"Error en {nombre}: {e}")
            return pd.DataFrame()
    
    def extraer_listings(self):
        return self.extraer_coleccion('listings')
    
    def extraer_reviews(self):
        return self.extraer_coleccion('reviews')
    
    def extraer_calendar(self):
        return self.extraer_coleccion('calendar')
    
    def cerrar(self):
        if self.client:
            self.client.close()
            self.logger.info("Conexión cerrada")
