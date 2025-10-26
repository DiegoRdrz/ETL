import logging
import pandas as pd
import numpy as np
from datetime import datetime
import os
import json


class Transformacion:
    """Limpia y transforma los datos extraídos"""
    
    def __init__(self):
        self._setup_log()
    
    def _setup_log(self):
        """Configura el log"""
        log_dir = os.path.join(os.path.dirname(__file__), '../logs')
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f'transformacion_{timestamp}.log')

        # Logger independiente
        self.logger = logging.getLogger(f"transformacion_{timestamp}")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            fh = logging.FileHandler(log_file)
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def _convertir_listas_a_json(self, df):
        """Convierte columnas con listas/diccionarios a strings JSON para SQLite"""
        columnas_convertidas = []
        for col in df.columns:
            # Verificar si la columna tiene listas o diccionarios
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
                columnas_convertidas.append(col)
        
        if columnas_convertidas:
            self.logger.info(f"Columnas convertidas a JSON: {columnas_convertidas}")
        
        return df
    
    def _eliminar_columnas_no_hashables(self, df):
        """Elimina columnas con listas/diccionarios que no se pueden usar en drop_duplicates"""
        columnas_problematicas = []
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                columnas_problematicas.append(col)
        
        if columnas_problematicas:
            self.logger.info(f"Eliminando columnas no hashables: {columnas_problematicas}")
            df = df.drop(columns=columnas_problematicas)
        
        return df
    
    def limpiar_duplicados(self, df, subset=None):
        """Elimina duplicados, manejando columnas con listas/diccionarios"""
        antes = len(df)
        
        # Si no se especifica subset, eliminar columnas problemáticas
        if subset is None:
            df = self._eliminar_columnas_no_hashables(df)
        
        df = df.drop_duplicates(subset=subset)
        despues = len(df)
        self.logger.info(f"Duplicados eliminados: {antes - despues}")
        return df
    
    def limpiar_nulos(self, df):
        """Elimina filas con muchos nulos"""
        antes = len(df)
        df = df.dropna(thresh=len(df.columns) * 0.5)
        despues = len(df)
        self.logger.info(f"Filas con nulos eliminadas: {antes - despues}")
        return df
    
    def normalizar_precio(self, df, columna='price'):
        """Normaliza precios: quita $, comas y convierte a float"""
        if columna in df.columns:
            df[columna] = df[columna].astype(str).str.replace('$', '').str.replace(',', '')
            df[columna] = pd.to_numeric(df[columna], errors='coerce')
            self.logger.info(f"Precio normalizado en columna {columna}")
        return df
    
    def convertir_fechas(self, df, columna='date'):
        """Convierte fechas a formato ISO"""
        if columna in df.columns:
            df[columna] = pd.to_datetime(df[columna], errors='coerce')
            self.logger.info(f"Fechas convertidas en columna {columna}")
        return df
    
    def derivar_variables_fecha(self, df, columna='date'):
        """Crea columnas de año, mes, día, trimestre"""
        if columna in df.columns and pd.api.types.is_datetime64_any_dtype(df[columna]):
            df['año'] = df[columna].dt.year
            df['mes'] = df[columna].dt.month
            df['dia'] = df[columna].dt.day
            df['trimestre'] = df[columna].dt.quarter
            self.logger.info("Variables de fecha derivadas")
        return df
    
    def categorizar_precio(self, df, columna='price'):
        """Categoriza precios en rangos"""
        if columna in df.columns:
            df['categoria_precio'] = pd.cut(df[columna], 
                                           bins=[0, 500, 1000, 2000, float('inf')],
                                           labels=['Bajo', 'Medio', 'Alto', 'Premium'])
            self.logger.info("Precios categorizados")
        return df
    
    def transformar_listings(self, df):
        """Aplica todas las transformaciones a listings"""
        self.logger.info(f"Transformando listings: {len(df)} registros")
        
        # Eliminar _id de MongoDB si existe
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
            self.logger.info("Columna _id eliminada")
        
        # Usar 'id' para detectar duplicados si existe
        subset_duplicados = ['id'] if 'id' in df.columns else None
        df = self.limpiar_duplicados(df, subset=subset_duplicados)
        
        df = self.limpiar_nulos(df)
        df = self.normalizar_precio(df, 'price')
        df = self.categorizar_precio(df, 'price')
        
        # Convertir listas/diccionarios a JSON para SQLite
        df = self._convertir_listas_a_json(df)
        
        self.logger.info(f"Listings transformado: {len(df)} registros finales")
        return df
    
    def transformar_reviews(self, df):
        """Aplica transformaciones a reviews"""
        self.logger.info(f"Transformando reviews: {len(df)} registros")
        
        # Eliminar _id de MongoDB si existe
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
            self.logger.info("Columna _id eliminada")
        
        # Usar 'id' para detectar duplicados si existe
        subset_duplicados = ['id'] if 'id' in df.columns else None
        df = self.limpiar_duplicados(df, subset=subset_duplicados)
        
        # Limpiar nulos (aunque reviews tiene pocos)
        df = self.limpiar_nulos(df)
        
        df = self.convertir_fechas(df, 'date')
        df = self.derivar_variables_fecha(df, 'date')
        self.logger.info(f"Reviews transformado: {len(df)} registros finales")
        return df
    
    def transformar_calendar(self, df):
        """Aplica transformaciones a calendar"""
        self.logger.info(f"Transformando calendar: {len(df)} registros")
        
        # Eliminar _id de MongoDB si existe
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
            self.logger.info("Columna _id eliminada")
        
        # Usar combinación de listing_id y date para duplicados
        subset_duplicados = ['listing_id', 'date'] if 'listing_id' in df.columns and 'date' in df.columns else None
        df = self.limpiar_duplicados(df, subset=subset_duplicados)
        
        # Limpiar nulos
        df = self.limpiar_nulos(df)
        
        df = self.convertir_fechas(df, 'date')
        df = self.derivar_variables_fecha(df, 'date')
        # Calendar no tiene columna price, solo available, minimum_nights, maximum_nights
        self.logger.info(f"Calendar transformado: {len(df)} registros finales")
        return df
