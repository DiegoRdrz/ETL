# INFORME
[Informe ETL](https://github.com/DiegoRdrz/ETL)

# ETL Airbnb CDMX

Proyecto ETL para análisis de datos de Airbnb Ciudad de México.

## Descripción

Proceso completo de Extracción, Transformación y Carga (ETL) de datos de Airbnb CDMX:

- **Extracción:** Desde MongoDB (base de datos `Airbnb_mx`)
- **Transformación:** Limpieza, normalización y creación de variables derivadas
- **Carga:** SQLite y archivos Excel

## Estructura del Proyecto

```
etl_airbnb/
├── src/
│   ├── extraccion.py       # Extracción desde MongoDB
│   ├── transformacion.py   # Limpieza y transformación
│   └── carga.py            # Carga a SQLite/Excel
├── notebooks/
│   └── exploracion_airbnb.ipynb  # Análisis exploratorio y ETL completo
├── data/                   # Datos procesados (SQLite y Excel)
├── logs/                   # Logs de ejecución
└── requirements.txt        # Dependencias
```

## Instalación

1. Clonar el repositorio
2. Crear entorno virtual:

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
```

3. Instalar dependencias:

```bash
pip install -r requirements.txt
```

## Ejecución

### Ejecutar ETL Completo

Abrir y ejecutar el notebook:

```bash
jupyter notebook notebooks/exploracion_airbnb.ipynb
```

El notebook incluye:

1. Extracción de datos desde MongoDB
2. Análisis exploratorio (info, nulos, duplicados, estadísticas)
3. Transformación de datos
4. Visualizaciones (distribución de precios, categorías, tendencias)
5. Carga a SQLite y Excel
6. Consultas SQL de ejemplo

## Transformaciones Aplicadas

### Listings

- Eliminación de duplicados (por ID)
- Limpieza de valores nulos (>50% columnas vacías)
- Normalización de precios: `"$3,799.00"` → `3799.0`
- Categorización de precios: Bajo/Medio/Alto/Premium
- Conversión de listas a JSON (amenities, host_verifications)

### Reviews

- Eliminación de duplicados (por ID)
- Limpieza de valores nulos
- Estandarización de fechas
- Variables derivadas: año, mes, día, trimestre

### Calendar

- Eliminación de duplicados (por listing_id + date)
- Limpieza de valores nulos
- Estandarización de fechas
- Variables derivadas: año, mes, día, trimestre

## Resultados

### Datos Procesados

- **Listings:** 26,401 propiedades
- **Reviews:** 1,388,226 reseñas
- **Calendar:** 9,636,365 registros de disponibilidad

### Archivos Generados

- `data/airbnb.db` - Base de datos SQLite con 3 tablas
- `data/listings_limpio.xlsx` - Propiedades procesadas
- `data/reviews_muestra.xlsx` - Muestra de 10,000 reviews

## Tecnologías

- Python 3.x
- MongoDB (pymongo)
- Pandas
- SQLite3
- Matplotlib/Seaborn
- Jupyter Notebook
