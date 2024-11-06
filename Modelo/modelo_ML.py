import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# movies_filt = pd.read_parquet('../datasets/movie_modelo.parquet')
movies_filt = pd.read_parquet('datasets/movie_modelo.parquet', engine= 'pyarrow')
# Normalización de popularity, budget, revenue y vote_average
df= movies_filt
# Supón que tienes el DataFrame `df` con las columnas mencionadas
scaler = MinMaxScaler()
df[['popularity', 'budget', 'revenue', 'vote_average']] = scaler.fit_transform(
    df[['popularity', 'budget', 'revenue', 'vote_average']]
)


# Procesamiento de overview (sinopsis)

# Limita la cantidad de palabras para reducir dimensiones
tfidf = TfidfVectorizer(max_features=100)
tfidf_matrix = tfidf.fit_transform(df['overview']).toarray()


# Concatenación de todas las características
# Combina las características numéricas y los vectores TF-IDF de la sinopsis
X = np.concatenate([
    df[['popularity', 'budget', 'revenue', 'vote_average']].values,
    tfidf_matrix
], axis=1)


# SIMILITUD DEL COSENO

# Calcula la matriz de similitud de coseno
cosine_sim = cosine_similarity(X)

# Función para obtener las películas más similares
def recomendacion(movie, cosine_sim_matrix, df, top_n=5):
    """
    Devuelve una lista numerada con las películas más similares a la película especificada.
    
    Inputs:
    - movie_index (int): Índice de la película base en el DataFrame.
    - cosine_sim_matrix (array): Matriz de similitud de coseno.
    - df (DataFrame): DataFrame que contiene las películas.
    - top_n (int): Número de recomendaciones (default=5).
    
    Outputs:
    - list: Lista de recomendaciones en formato numerado.
    """
   # Verifica si el título de la película existe en el DataFrame
    if movie not in df['title'].values:
        return [f"La película '{movie}' no se encontró en la base de datos."]
    
    # Obtiene el índice de la película con el título dado
    movie_index = df[df['title'] == movie].index[0]


    # Calcula la similitud de la película base con todas las demás
    sim_scores = list(enumerate(cosine_sim_matrix[movie_index]))
    
    # Ordena las películas por similitud (de mayor a menor) y omite la película base
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_indices = [i[0] for i in sim_scores[1:top_n+1]]
    
    # Extrae los títulos de las películas recomendadas
    titulos_recom = df.iloc[sim_indices]['title'].tolist()
    
    # Formatea la lista en formato numerado
    list_enumerada = [f"{i+1}. {title}" for i, title in enumerate(titulos_recom)]
    
    return list_enumerada
# Ejemplo: muestra las 5 películas más similares a la del índice 0
# funcion_ml = recomendacion('Toy Story', cosine_sim, df)

# for movie in funcion_ml:
#     print(movie)