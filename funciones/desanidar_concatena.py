
# Función para desanidar <p>
# **Párametros de entrada**: dataframe y columna a desanidar

# 1. Se extraer los valores del str
# 2. Verificar si es lista/diccionario
# 3. Normalizar columnas
# 4. Cambiar el nombre de columnas normalizadas
# 5. Elimina la columna original
# 6. Concatenar las nuevas columnas al dataframe final

import ast
import pandas as pd
from pandas import json_normalize
import sys
import os
sys.path.append(os.path.abspath(".."))


def desanidar_concat(df, col):
    # Convertir cadenas de texto en listas/diccionarios si es necesario
    df[col] = df[col].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    
    # Explode para listas de diccionarios o manejar diccionarios directamente
    if df[col].apply(lambda x: isinstance(x,list)).any(): 
        col_norm = pd.json_normalize(df[col].explode().dropna()).reset_index(drop=True)
    else: # para diccionarios
        col_norm = pd.json_normalize(df[col].dropna()).reset_index(drop=True)
    
    # Rename las nuevas columnas
    col_norm = col_norm.add_suffix(f'_{col}')

    #Elimina columna original, agrega columnas nuevas al dataframe
    #df = pd.concat([df.drop(columns=[col]) , col_norm], axis=1)
    new_df= df.drop(columns=col).join(col_norm)

    return new_df

