from fastapi import FastAPI
import pandas as pd
import uvicorn
import os
from fastapi.responses import JSONResponse
from Modelo.modelo_ML import recomendacion  # Importar la función de recomendación
from Modelo.modelo_ML import cosine_sim

app = FastAPI()

movie_api = pd.read_parquet('datasets/movie_dataset_final.parquet', engine= 'pyarrow')
credits_cast = pd.read_parquet('datasets/credits_cast.parquet', engine= 'pyarrow')
credits_crew = pd.read_parquet('datasets/credits_crew.parquet', engine= 'pyarrow')
movies_filt = pd.read_parquet('datasets/movie_modelo.parquet', engine= 'pyarrow')

#FUNCIONES
def f_filmaciones_dia(df,day,column ):
    dias_semana = {0:'Lunes', 1:'Martes', 2:'Miercoles', 3:'Jueves', 4: 'Viernes', 
               5:'Sabado', 6:'Domingo'}
    day = day.capitalize()

    if day not in dias_semana.values():
        return f"Error: {day} no es un día válido. Ingrese un día de la semana en español."


    df['release_dia'] =df[column].dt.day_of_week.map(dias_semana)
    films_day =  df[df['release_dia']== day]
    len_films = len(films_day)
    return f"{len_films} cantidad de películas fueron estrenadas en los días {day}"

def f_score_titulo( df, titulo ):
    #Filtra fila con el titulo de la pelicula
    titulo=titulo.title()

    film_row = df[ df["title"] == titulo]

    if film_row.empty:
        return f"Error: Película {titulo} no encontrada."
    
    anio_estreno= film_row["release_date"].dt.year.values[0]
    score = film_row["popularity"].values[0]

    return f"La película '{titulo}' fue estrenada en el año {anio_estreno} con un score/popularidad de {score}"

def f_votos_titulo( df, titulo ):

    titulo=titulo.title()
    #Filtra fila con el titulo de la pelicula
    film_row = df[ df["title"] == titulo]

    if film_row.empty:
        return f"Error: Película {titulo} no encontrada."
    
    anio_estreno= film_row["release_date"].dt.year.values[0]
    cant_voto= film_row["vote_count"].values[0]
    prom_voto = film_row["vote_average"].values[0]

    if cant_voto >=2000 :
        return f"La película {titulo} fue estrenada en el año {anio_estreno}. La misma cuenta con un total de {cant_voto} valoraciones, con un promedio de {prom_voto}"

    else:
        return f"La película {titulo} no cuenta con más de 2000 valoraciones. "

def f_get_actor(df, df_cast, actor):
    actor = actor.title()

    # Filtrar el dataframe de actores con el nombre ingresado
    df_actor  = df_cast[df_cast['name_actor'] == actor]

    if df_actor .empty:
        return {"message": f"El actor '{actor}' no se encuentra en la base de datos."}

    # Se obtienen los ids de las películas en las que el actor ha participado
    cantidad_peliculas = df_actor ['id_movie'].nunique()

    # Obtener los ids de las películas
    movies = df_actor ['id_movie'].unique()

    # Coincidir con el dataframe de películas
    verifcacion = df[df['id_movie'].isin(movies)]

    # Calcular el retorno total sumando la columna 'return' de las películas
    retorno = round(verifcacion['return'].sum(), 2)

    # Calcular el promedio de retorno dividiendo el retorno total por la cantidad de películas
    promedio = round(verifcacion['return'].mean(), 2)

    # Retornar los resultados
    return {
        "message": (f"El actor {actor} ha participado de {cantidad_peliculas} filmaciones, "
                    f"el mismo ha conseguido un retorno de {retorno} veces la inversión. "
                    f"Con un promedio de {promedio} por filmación.")
    }

def f_get_director(df_movies, df_crew, director_name):
    # Convertimos el nombre del director a título para una comparación uniforme
    director_name = director_name.title()

    # Filtrar solo directores en el dataset de crew
    df_directors = df_crew[df_crew["job"] == 'Director']

    # Unir datasets usando la columna `id_movie`
    merged_df = df_movies.merge(df_directors, on='id_movie', how='inner')

    # Filtrar el dataframe unido por el nombre del director
    df_director_movies = merged_df[merged_df["name_dir"] == director_name]

    if df_director_movies.empty:
        return {"message": f"El director '{director_name}' no se encuentra en la base de datos. Por favor, ingrese otro nombre."}

    # Calcular el retorno total y el promedio de retorno
    retorno_total = round(df_director_movies['return'].sum(), 2)
    promedio_retorno = round(df_director_movies['return'].mean(), 2)

    # Usar .apply() para crear la lista de películas con sus detalles
    peliculas = df_director_movies.apply(
        lambda row: {
            "Titulo": row['title'],
            "Fecha de Estreno": row['release_year'],
            "Costo": row['budget'],
            "Recaudación": row['revenue'],
            "Ganancia Neta": row['revenue'] - row['budget'],
            "Retorno": row['return']
        }, axis=1 
        ).tolist()

    return {
        "Director": director_name,
        "Retorno Total": retorno_total,
        "Promedio Retorno": promedio_retorno,
        "Películas": peliculas
    }

# API
@app.get("/inicio")
async def ruta_prueba():
    return "Hola"

@app.get("/Cantidad_Filmaciones_Mes/{mes}")
async def cantidad_filmaciones_mes(mes: str):
    """
    Input:
    - Mes del año. (str)

    Output:
    - Mensaje: 'X cantidad de películas fueron estrenadas en el mes de X.'
    """

    # Diccionario de mapeo para los meses
    meses = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo',
        6: 'Junio', 7: 'Julio', 8: 'Agosto', 9: 'Septiembre',
        10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }
    
    # Convertir el nombre del mes a capitalizado para asegurar la correspondencia
    mes = mes.capitalize()
    
    # Verificar si el nombre del mes es válido
    if mes not in meses.values():
        return {"message": f"Error: {mes} no es un mes válido. Ingrese un mes en español."}
    
    # Obtener el número del mes
    mes_numero = {v: k for k, v in meses.items()}[mes]
    
    # Filtrar el DataFrame para el mes deseado
    movie_api['release_mes'] = movie_api['release_date'].dt.month  # Extraer el número del mes
    films_mes = movie_api[movie_api['release_mes'] == mes_numero]
    
    # Contar la cantidad de películas en el mes
    len_films = len(films_mes)
    
    return {"message": f"{len_films} cantidad de películas fueron estrenadas en el mes de {mes}"}


@app.get("/Cantidad_Filmaciones_Dia/{dia}")
async def cantidad_filmaciones_dia(dia:str):
    """
    Input:
    - Día de la semana. (str)

    Output:
    - Mensaje: 'X cantidad de películas fueron estrenadas en los días X.'
    """
    return {"message":f_filmaciones_dia(movie_api,dia,'release_date')}

@app.get("/Score_Titulo/{titulo}")
async def Score_Titulo(titulo:str):
    """
    Input:
    - Titulo de la película. (str)

    Output:
    - Mensaje: 'La película X fue estrenada en el año X con un score/popularidad de X.'
    """
    return {"message":f_score_titulo(movie_api,titulo)}

@app.get("/Votos_Titulo/{titulo}")
async def Votos_Titulo(titulo:str):
    """
    Input:
    - Titulo de la película. (str)

    Output:
    - Mensaje: 'La película X fue estrenada en el año X. La misma cuenta con un total de X valoraciones, con un promedio de X'
    """
    return {"message":f_votos_titulo(movie_api,titulo)}

@app.get("/Get_Actor/{actor}")
async def Get_Actor(actor:str):
    """
    Input:
    - Nombre del actor (str)

    Output:
    - Mensaje: 'El actor X ha participado de X cantidad de filmaciones, el mismo ha conseguido un retorno de X con un promedio de X por filmación'
     """
    message = f_get_actor(movie_api, credits_cast, actor)
    return {"message":message}


@app.get("/Get_Director/{director}")
async def Get_Director(director:str):
    """
    Input:
    - Nombre del director (str)

    Output:
    - Nombre del director.
    - Promedio Éxito.
    - Lista de películas
        - Titulo
        - Fecha de estreno
        - Costo 
        - Recaudación
        - Ganancia Neta
        - Retorno 

     """
    message = f_get_director(movie_api, credits_crew, director)
    return {"message":message}


@app.get("/Recomendacion/{titulo}")
def Recomendacion(titulo:str):
    """
    Input:
    - Titulo de la película (str)

    Output:
    - Lista de películas recomendadas
    """
    lista_movies = recomendacion(titulo, cosine_sim, movies_filt)

    # for movie in funcion_ml:
    # print(movie)

    # if lista_movies == 0: 
    #     return f"Error: Película '{titulo}' no encontrada en el dataset."

    return {"message":lista_movies}
#

