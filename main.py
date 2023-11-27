import uvicorn
from fastapi import FastAPI
import pandas as pd
from fastapi.responses import JSONResponse





app = FastAPI()

ruta_archivo = 'Data/TimeGenre.parquet'  # Reemplaza con la ruta a tu archivo

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

@app.get("/PlayTimeGenre/{genero}")
async def PlayTimeGenre(genero: str):
    # Filtrar el dataframe por el género específico
    df_genre = horas_usuario[horas_usuario['genres'] == genero]

    # Si no hay datos para el género, devolver un mensaje indicando esto
    if df_genre.empty:
        return f"No hay datos para el género '{genero}'."

    # Encontrar el año con más horas jugadas para el género específico
    año_con_mas_horas = df_genre.groupby('Año_estreno')['playtime_forever'].sum().idxmax()

    # Construir el mensaje con el año y el género
    mensaje = f"Año de lanzamiento con más horas jugadas para Género '{genero}': {año_con_mas_horas}"

    return mensaje