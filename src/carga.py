import logging
import pandas as pd
import sqlite3
from datetime import datetime
import os


class Carga:
    """Carga datos transformados a SQLite y Excel"""
    
    def __init__(self, db_path='../data/airbnb.db'):
        self.db_path = os.path.join(os.path.dirname(__file__), db_path)
        self._setup_log()
        self._crear_directorio()
    
    def _setup_log(self):
        """Configura el log"""
        log_dir = os.path.join(os.path.dirname(__file__), '../logs')
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f'carga_{timestamp}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)
    
    def _crear_directorio(self):
        """Crea directorio data si no existe"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def cargar_a_sqlite(self, df, tabla):
        """Carga DataFrame a SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            df.to_sql(tabla, conn, if_exists='replace', index=False)
            conn.close()
            self.logger.info(f"Cargados {len(df)} registros a tabla {tabla} en SQLite")
            return True
        except Exception as e:
            self.logger.error(f"Error cargando a SQLite: {e}")
            return False
    
    def cargar_a_excel(self, df, nombre_archivo):
        """Carga DataFrame a Excel"""
        try:
            excel_dir = os.path.join(os.path.dirname(__file__), '../data')
            os.makedirs(excel_dir, exist_ok=True)
            
            excel_path = os.path.join(excel_dir, nombre_archivo)
            df.to_excel(excel_path, index=False, engine='openpyxl')
            self.logger.info(f"Cargados {len(df)} registros a {nombre_archivo}")
            return True
        except Exception as e:
            self.logger.error(f"Error cargando a Excel: {e}")
            return False
    
    def verificar_carga(self, tabla):
        """Verifica que los datos se cargaron correctamente"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"SELECT COUNT(*) FROM {tabla}"
            count = pd.read_sql_query(query, conn).iloc[0, 0]
            conn.close()
            self.logger.info(f"Verificación: {count} registros en tabla {tabla}")
            return count
        except Exception as e:
            self.logger.error(f"Error verificando carga: {e}")
            return 0
