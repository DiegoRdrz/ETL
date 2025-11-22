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

        # Logger independiente
        self.logger = logging.getLogger(f"carga_{timestamp}")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            fh = logging.FileHandler(log_file)
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

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
            
            # Crear una copia para no modificar el original
            df_excel = df.copy()
            
            # Limitar el número de filas por el límite de Excel
            MAX_ROWS = 1048575
            if len(df_excel) > MAX_ROWS:
                print(f"{nombre_archivo.split('.')[0]} tiene {len(df_excel):,} registros, exportando primeros {MAX_ROWS:,}...")
                df_excel = df_excel.head(MAX_ROWS)
            
            # CRÍTICO: Truncar textos largos que causan problemas en Excel
            MAX_CELL_LENGTH = 32767  # Límite de Excel por celda
            for col in df_excel.select_dtypes(include=['object']).columns:
                # Limpiar caracteres problemáticos
                df_excel[col] = df_excel[col].astype(str).str.replace('<br/>', ' ', regex=False)
                df_excel[col] = df_excel[col].str.replace('<br>', ' ', regex=False)
                df_excel[col] = df_excel[col].str.replace('\n', ' ', regex=False)
                df_excel[col] = df_excel[col].str.replace('\r', ' ', regex=False)
                
                # Truncar textos muy largos
                df_excel[col] = df_excel[col].str[:MAX_CELL_LENGTH]
            
            # Convertir columnas datetime a string
            for col in df_excel.columns:
                if pd.api.types.is_datetime64_any_dtype(df_excel[col]):
                    df_excel[col] = df_excel[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
            
            # Asegurarse de que las columnas numéricas derivadas sean enteros
            for col in ['año', 'mes', 'dia', 'trimestre']:
                if col in df_excel.columns:
                    df_excel[col] = df_excel[col].fillna(0).astype(int)
            
            excel_path = os.path.join(excel_dir, nombre_archivo)
            
            # Usar xlsxwriter que es más robusto para archivos grandes
            try:
                df_excel.to_excel(excel_path, index=False, engine='xlsxwriter')
            except:
                # Si xlsxwriter falla, intentar con openpyxl
                df_excel.to_excel(excel_path, index=False, engine='openpyxl')
            
            self.logger.info(f"✓ Exportados {len(df_excel):,} registros a {nombre_archivo}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error cargando a Excel: {str(e)[:200]}")  # Truncar el mensaje de error
            print(f"ERROR al exportar {nombre_archivo}: {str(e)[:200]}")
            
            # Intentar guardar como CSV como backup
            try:
                csv_path = excel_path.replace('.xlsx', '.csv')
                df_excel.to_csv(csv_path, index=False, encoding='utf-8-sig')
                print(f"✓ Guardado como CSV alternativo: {os.path.basename(csv_path)}")
                self.logger.info(f"✓ Guardado como CSV alternativo: {os.path.basename(csv_path)}")
                return True
            except Exception as e2:
                self.logger.error(f"Error también al guardar CSV: {str(e2)[:200]}")
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
