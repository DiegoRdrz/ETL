# INFORME

[Informe ETL](https://docs.google.com/document/d/1riNNTxp6XgctT9ooaYw2YByyfvXwraZi_qkSoxFuGho/edit?usp=sharing) Taller 2

[Informe ETL ML](https://docs.google.com/document/d/1uej3OoBZzbXf_cKlJySMJ3MUDMVWSNzhDG2W7XUDDic/edit?usp=sharing) Taller Final

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

## Extensión del Proyecto: Visualización y Modelos de Machine Learning

## Taller Final

A partir de la base de datos procesada en `data/airbnb.db` se desarrolló una capa adicional de análisis que integra **visualización interactiva** y **modelos de Machine Learning** para responder preguntas de negocio sobre el mercado de Airbnb en Ciudad de México.

En la carpeta [notebooks/](cci:7://file:///c:/Users/DiegoRdrz/Desktop/code/ETL/notebooks:0:0-0:0) se añadieron tres cuadernos Jupyter:

- [1_Regresion.ipynb](cci:7://file:///c:/Users/DiegoRdrz/Desktop/code/ETL/notebooks/1_Regresion.ipynb:0:0-0:0): modelo de **regresión** (Random Forest Regressor) para estimar el precio por noche a partir de características del alojamiento, su ubicación y su historial de reseñas.
- [2_Clasificacion.ipynb](cci:7://file:///c:/Users/DiegoRdrz/Desktop/code/ETL/notebooks/2_Clasificacion.ipynb:0:0-0:0): modelo de **clasificación** (Logistic Regression) para analizar el nivel de demanda de los listings usando `number_of_reviews` como proxy.
- [3_Clustering.ipynb](cci:7://file:///c:/Users/DiegoRdrz/Desktop/code/ETL/notebooks/3_Clustering.ipynb:0:0-0:0): modelo de **clustering** (K-Means) para segmentar las propiedades en grupos homogéneos (estándar, premium y estancias largas) según precio, tamaño, disponibilidad y localización.

Adicionalmente, se creó la carpeta `visualizacion/`, donde se puede incluir el tablero de **Power BI** construido a partir del dataset resultante del ETL. Este tablero contiene visualizaciones clave para cada pregunta de negocio (relación entre capacidad y precio, demanda por categoría de precio y concentración geográfica de alojamientos de alto valor), lo que permite explorar de forma interactiva los hallazgos obtenidos en los notebooks de ML y facilitar la toma de decisiones basada en datos.

## Tecnologías

- Python 3.x
- MongoDB (pymongo)
- Pandas
- SQLite3
- Matplotlib/Seaborn
- Jupyter Notebook
- scikit-learn
- NumPy
