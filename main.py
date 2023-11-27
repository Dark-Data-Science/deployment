import uvicorn
from fastapi import FastAPI
import pandas as pd
from fastapi.responses import JSONResponse





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




df_horas_usuario = pd.read_parquet('Data/ForGenre.parquet')

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


