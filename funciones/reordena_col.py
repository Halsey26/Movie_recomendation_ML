

def reordena_colum (df,col ,indice):
    # Obtener todas las columnas en una lista
    columnas = df.columns.tolist()

    # Mover 'id_movie' al principio de la lista
    columnas.insert(indice, columnas.pop(columnas.index(col)))

    # Reorganizar el DataFrame usando la nueva lista de columnas
    df = df[columnas]

    return df