# Proyecto_Individual_1

## Descripción

Este proyecto es una API construida con **FastAPI** que permite realizar consultas sobre un dataset de películas. Las funciones de la API proporcionan información relacionada con películas, actores, y directores, incluyendo cantidad de filmaciones por mes y día, popularidad, y retorno de inversión. Además, cuenta con transformaciones de datos necesarias para procesar el dataset.

El proyecto incluye un modelo de Machine Learning para recomendaciones y permite hacer consultas basadas en los siguientes puntos:

- Géneros de una película
- Votos de una película
- Popularidad de una película

## Estructura del Proyecto

```
main/
│
├── ETL_EDA/
│   ├── EDA.ipynb                  # Script para EDA de los datos de movie y generos
│   ├── ETL_movie.ipynb            # Script para ETL de los datos de movie
│   ├── ETL_credits.ipynb          # Script para ETL de los datos de credits
│
├── Modelo/
│   ├── modelo_ML.py               # Script con modelo de Machine Learning
│   ├── dataset_modelo.ipynb       # Preprocesamiento del dataset para el modelo
│
├── datasets/
│   ├── credits_cast.parquet       # Datos de los actores
│   ├── credits_crew.parquet       # Datos del equipo de directores
│   ├── movie_dataset_final.parquet # Dataset final de movie
│   ├── movie_modelo.parquet       # Dataset filtrado para el modelo
│
├── .gitignore                     # Archivos y carpetas ignorados en Git
├── main.py                        # Script principal de la API
├── README.md                      # Documentación del proyecto
├── requirement.txt                # Lista de dependencias del proyecto
```

## Requerimientos

Antes de comenzar, asegúrate de tener instalado lo siguiente:

- Python 3.8 o superior
- FastAPI
- Pandas
- Pyarrow
- Uvicorn (para ejecutar el servidor de FastAPI)
  
Puedes instalar todas las dependencias ejecutando:

```bash
pip install -r requirements.txt
```

## Transformaciones de datos

Las siguientes transformaciones se han aplicado al dataset:

- **Desanidamiento**: Algunos campos como `belongs_to_collection`, `production_companies` se han desanidado para facilitar las consultas.
- **Valores nulos**: Los campos `revenue` y `budget` con valores nulos se han rellenado con 0. Las fechas nulas en `release_date` se eliminaron.
- **Formato de fecha**: Las fechas en el campo `release_date` se han estandarizado al formato `AAAA-mm-dd`. Se ha creado una columna `release_year` que extrae el año de la fecha de estreno.
- **Columna `return`**: Se ha añadido una columna que calcula el retorno de inversión (`return`) dividiendo `revenue` por `budget`. Si los datos no están disponibles, el valor es 0.
- **Columnas eliminadas**: Se eliminaron las columnas `video`, `imdb_id`, `adult`, `original_title`, `poster_path`, y `homepage` ya que no son necesarias.

## Endpoints

La API cuenta con los siguientes endpoints:

- **GET `/cantidad_filmaciones_mes/{mes}`**: Devuelve la cantidad de películas estrenadas en un mes específico.
  - Ejemplo: `X cantidad de películas fueron estrenadas en el mes de {mes}`

- **GET `/cantidad_filmaciones_dia/{dia}`**: Devuelve la cantidad de películas estrenadas en un día específico.
  - Ejemplo: `X cantidad de películas fueron estrenadas en los días {dia}`

- **GET `/score_titulo/{titulo}`**: Devuelve el título, año de estreno y score de una película.
  - Ejemplo: `La película {titulo} fue estrenada en el año {año} con un score/popularidad de {score}`

- **GET `/votos_titulo/{titulo}`**: Devuelve el título, cantidad de votos y promedio de las votaciones si la película tiene al menos 2000 valoraciones.
  - Ejemplo: `La película {titulo} fue estrenada en el año {año}. La misma cuenta con un total de {votos} valoraciones, con un promedio de {promedio}`

- **GET `/get_actor/{actor}`**: Devuelve la cantidad de películas de un actor, su éxito medido por retorno y promedio de retorno por filmación.
  - Ejemplo: `El actor {actor} ha participado de {cantidad} filmaciones, obteniendo un retorno total de {retorno} con un promedio de {promedio}`

- **GET `/get_director/{director}`**: Devuelve el éxito de un director medido por retorno, además de la información de cada película dirigida por él.
  - Ejemplo: `{película}, lanzada en {fecha}, tuvo un retorno de {retorno}, con un costo de {costo} y una ganancia de {ganancia}`

## Ejecución

Para ejecutar la API localmente, usa el siguiente comando:

```bash
uvicorn main:app --reload
```

Esto levantará un servidor local donde podrás hacer consultas a los endpoints.

## Despliegue

Para desplegar este proyecto en Render o cualquier otro servicio de cloud:

1. Asegúrate de tener configurado el archivo `requirements.txt`.
2. Sube tus commits a tu repositorio de GitHub.
3. Sigue los pasos de despliegue en Render o el servicio que prefieras.

## Contribuciones

Si deseas contribuir a este proyecto, realiza un **fork** del repositorio y crea una nueva rama para tus modificaciones. Luego envía un **pull request** para revisión.
