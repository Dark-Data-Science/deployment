from fastapi import FastAPI, HTTPException
import pandas as pd
import fastparquet
import dask.dataframe as dd
import dask
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity





app = FastAPI()

ruta_archivo = 'Data/genero_play_time.parquet'  # Reemplaza con la ruta a tu archivo

# Leer los datos en un dataframe
horas_usuario = pd.read_parquet(ruta_archivo)

# Ruta para obtener el año de lanzamiento con más horas jugadas para un género específico.
# Parámetros:
#   - genero (str): Género para el cual se desea obtener la información.
#                   Debe ingresarse el genero del juego de la columna "genre" en formato string.
# Respuestas:
#   - Si se encuentran datos para el género, se devuelve un mensaje con el año de lanzamiento
#     con más horas jugadas. Ejemplo: "Año de lanzamiento con más horas jugadas para Género 'Accion': 2013".
#   - Si no hay datos para el género especificado, se devuelve un mensaje indicando la ausencia de datos.
#     Ejemplo: "No hay datos para el género 'Accion'."


# Asegúrate de cargar tu DataFrame 'horas_usuario' aquí
# Reemplaza esto con el código para cargar tu DataFrame

@app.get("/PlayTimeGenre/{genero}")
async def PlayTimeGenre(genero: str):
    # Filtra el DataFrame por el género especificado
    df_genero = horas_usuario[horas_usuario['genres'].str.contains(genero, na=False)]
    
    # Agrupa por 'Año_estreno' y suma el tiempo de juego
    df_grouped = df_genero.groupby('Año_estreno')['playtime_forever'].sum()
    
    # Encuentra el año con el tiempo de juego más alto
    max_playtime_year = df_grouped.idxmax()
    
    return {"Año con más horas jugadas para el género": genero, "Año": max_playtime_year}




df_horas_usuario = pd.read_parquet('Data/user_for_genero_mitad.parquet')

@app.get("/UserForGenre/{genero}")
async def UserForGenre(genero: str):
    # Filtrar el dataframe por el género específico
    df_genre = df_horas_usuario[df_horas_usuario['genres'] == genero]

    # Si no hay datos para el género, devolver un mensaje indicando esto
    if df_genre.empty:
        return {"mensaje": f"No hay datos para el género '{genero}'."}

    # Encontrar el usuario con más horas jugadas para el género específico
    usuario_con_mas_horas = df_genre.groupby('user_id')['playtime_forever'].sum().idxmax()

    # Crear una lista de la acumulación de horas jugadas por año
    acumulacion_por_anio = df_genre.groupby('Año_estreno')['playtime_forever'].sum().reset_index()
    acumulacion_por_anio = acumulacion_por_anio.rename(columns={"playtime_forever": "Horas"})
    lista_acumulacion = acumulacion_por_anio.to_dict(orient='records')

    # Crear la salida en forma de diccionario
    salida = {
        "Usuario con más horas jugadas para Género": usuario_con_mas_horas,
        "Horas jugadas": lista_acumulacion
    }

    return salida






recomendado = pd.read_parquet('Data/Recommend_Users2.parquet')

@app.get('/UsersRecommend/{anio}')
async def UsersRecommend(anio: int):
    try:
        # Filtrar por el año dado
        df_year = recomendado[recomendado['Año_estreno'] == anio]

        # Verificar si hay datos para el año dado
        if df_year.empty:
            raise HTTPException(status_code=404, detail=f"No hay datos para el año {anio}.")

        # Filtrar por recomendaciones positivas y comentarios positivos/neutrales
        df_recommendations = df_year[(df_year['recommend'] == True) & (df_year['sentiment_analisis'] >= 1)]

        # Verificar si hay juegos recomendados para el año dado
        if df_recommendations.empty:
            raise HTTPException(status_code=404, detail=f"No hay juegos recomendados para el año {anio}.")

        # Agrupar por el nombre del juego y sumar el 'sentiment_analisis'
        grouped_games = df_recommendations.groupby('item_name')['sentiment_analisis'].sum()

        # Obtener el top 3 de juegos más recomendados
        top_3_games = grouped_games.nlargest(3)

        # Crear la lista de salida en el formato deseado
        output_list = [{"Puesto 1": top_3_games.index[0]},
                       {"Puesto 2": top_3_games.index[1]},
                       {"Puesto 3": top_3_games.index[2]}]

        return output_list

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Se produjo un error inesperado: {str(e)}")
    



df_recomendado = pd.read_parquet('Data/Recommend_Users2.parquet')



@app.get('/UsersWorstDeveloper/{anio}')
async def UsersWorstDeveloper(anio: int):
    try:
        # Filtrar por el año dado
        df_year = df_recomendado[df_recomendado['Año_estreno'] == anio]

        # Filtrar por juegos menos recomendados y comentarios negativos
        df_not_recommendations = df_year[(df_year['recommend'] == False) & (df_year['sentiment_analisis'] == 0)]

        # Verificar si hay datos para el año dado y los filtros especificados
        if df_not_recommendations.empty:
            raise HTTPException(status_code=404, detail=f"No hay datos de juegos menos recomendados para el año {anio} con los filtros especificados.")
        
        # Contar la cantidad de veces que aparece cada juego menos recomendado
        juegos_menos_recomendados = df_not_recommendations['item_name'].value_counts().reset_index()
        juegos_menos_recomendados.columns = ['item_name', 'count']

        # Obtener el top 3 de juegos menos recomendados
        top_juegos_menos_recomendados = juegos_menos_recomendados.nlargest(3, 'count')

        # Crear la lista de salida en el formato deseado
        resultado = [{"Puesto 1": top_juegos_menos_recomendados.iloc[0]['item_name']},
                     {"Puesto 2": top_juegos_menos_recomendados.iloc[1]['item_name']},
                     {"Puesto 3": top_juegos_menos_recomendados.iloc[2]['item_name']}]

        return resultado

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Se produjo un error inesperado: {str(e)}")
    


analisis_sentimiento = pd.read_parquet('Data/Sentiment.parquet')

@app.get('/sentiment_analysis/{desarrolladora}')
def sentiment_analysis(desarrolladora: str):
    # Mapeo de los valores de 'sentiment_analisis' a las cadenas de texto correspondientes
    sentiment_mapping = {0: 'Negative', 1: 'Neutral', 2: 'Positive'}

    # Filtrar el dataframe por la desarrolladora especificada
    df_developer = analisis_sentimiento[analisis_sentimiento['developer'] == desarrolladora]

    # Si no hay datos para la desarrolladora, devolver un mensaje indicando esto
    if df_developer.empty:
        return {desarrolladora: "No hay datos para la desarrolladora."}

    # Reemplazar los valores en 'sentiment_analisis' con las cadenas de texto correspondientes
    df_developer['sentiment_analisis'] = df_developer['sentiment_analisis'].map(sentiment_mapping)

    # Contar el número de registros de reseñas de usuarios que se encuentran categorizados con un análisis de sentimiento
    sentiment_counts = df_developer['sentiment_analisis'].value_counts().to_dict()

    # Crear la lista de salida en el formato deseado
    output_list = [f"{key} = {value}" for key, value in sentiment_counts.items()]

    return {desarrolladora: output_list}


modelado_item = pd.read_parquet('Data/Sentiment.parquet')


@app.get("/recomendacion/{id_producto}")
async def get_recomendacion(id_producto: int):
    # Filtrar el DataFrame modelado_item para obtener las recomendaciones de juegos
    # asociadas con el juego específico identificado por su id.
    recomendaciones = modelado_item[modelado_item['id'] == id_producto]['recomendaciones'].iloc[0]

    # Verificar si hay recomendaciones disponibles para el juego dado
    if len(recomendaciones) > 0:
        # Crear un diccionario numerado de recomendaciones para una presentación más organizada
        recomendaciones_dict = {i + 1: juego for i, juego in enumerate(recomendaciones)}
        # Devolver el diccionario de recomendaciones numeradas
        return recomendaciones_dict
    else:
        # Si no hay recomendaciones disponibles, devolver un diccionario vacío
        return {}






@app.get("/recomendacion_juego/{product_id}")
async def recomendacion_juego(product_id: int):
    try:
        analisis_sentimiento = pd.read_csv('Data/Sentiment.csv')

        target_game = analisis_sentimiento[analisis_sentimiento['item_id'] == product_id]

        if target_game.empty:
            return {"message": "No se encontró el juego de referencia."}

        target_game_tags_and_genres = ' '.join(target_game['tags'].fillna('').astype(str) + ' ' + target_game['genres'].fillna('').astype(str))

        tfidf_vectorizer = TfidfVectorizer()

        chunk_size = 100   
        similarity_scores = None

        for chunk in pd.read_csv('Data/Sentiment.csv', chunksize=chunk_size):
            chunk_tags_and_genres = chunk['tags'].fillna('').astype(str) + ' ' + chunk['genres'].fillna('').astype(str)
            games_to_compare = [target_game_tags_and_genres] + chunk_tags_and_genres.tolist()

            tfidf_matrix = tfidf_vectorizer.fit_transform(games_to_compare)

            if similarity_scores is None:
                similarity_scores = cosine_similarity(tfidf_matrix)
            else:
                similarity_scores = cosine_similarity(tfidf_matrix)

        if similarity_scores is not None:
            similar_games_indices = similarity_scores[0].argsort()[::-1]

            num_recommendations = 5
            recommended_games = analisis_sentimiento.loc[similar_games_indices[1:num_recommendations + 1]]

            return recommended_games[['app_name']].to_dict(orient='records')

        return {"message": "No se encontraron juegos similares."}

    except Exception as e:
        return {"message": f"Error: {str(e)}"}