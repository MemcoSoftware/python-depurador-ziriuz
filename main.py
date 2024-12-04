import pandas as pd
import os
import unicodedata
from openpyxl import Workbook

# Rutas de los archivos
ruta_clases = r'C:\Users\B4N3D or Banned\Documents\MEMCO\Ziriuz1\DEPURACIÓN ZIRIUZ\CLASES EQUIPOS\clases.csv'
ruta_marcas = r'C:\Users\B4N3D or Banned\Documents\MEMCO\Ziriuz1\DEPURACIÓN ZIRIUZ\MARCAS EQUIPOS\marcas.csv'

# Verificar si los archivos existen
if not os.path.exists(ruta_clases):
    print(f"Error: El archivo '{ruta_clases}' no existe.")
    exit()

if not os.path.exists(ruta_marcas):
    print(f"Error: El archivo '{ruta_marcas}' no existe.")
    exit()

# Cargar los archivos CSV
try:
    clases = pd.read_csv(ruta_clases, sep=',', encoding='utf-8', engine='python')
    marcas = pd.read_csv(ruta_marcas, sep=',', encoding='utf-8', engine='python')
except Exception as e:
    print(f"Error al cargar los archivos CSV: {e}")
    exit()

# Si los datos están en una sola columna, procesarlos correctamente
if len(clases.columns) == 1:
    clases = clases[clases.columns[0]].str.split(',', expand=True)
    clases.columns = ['id', 'activo', 'clase', 'id_preventivo']

if len(marcas.columns) == 1:
    marcas = marcas[marcas.columns[0]].str.split(',', expand=True)
    marcas.columns = ['id', 'activo', 'marca']

# Convertir `id_preventivo` e `id` a enteros
clases['id'] = pd.to_numeric(clases['id'], errors='coerce').fillna(0).astype(int)
clases['id_preventivo'] = pd.to_numeric(clases['id_preventivo'], errors='coerce').fillna(0).astype(int)
marcas['id'] = pd.to_numeric(marcas['id'], errors='coerce').fillna(0).astype(int)

# Normalizar texto: quitar tildes, convertir a minúsculas
def normalizar_texto(texto):
    return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8').lower()

# Normalizar columnas específicas
clases['clase'] = clases['clase'].astype(str).apply(normalizar_texto)
marcas['marca'] = marcas['marca'].astype(str).apply(normalizar_texto)

# Filtrar y remover campos vacíos o que contienen solo espacios/puntos
def es_valido(valor):
    return bool(valor.strip().replace('.', ''))

clases_validas = clases[clases['clase'].apply(es_valido)]
marcas_validas = marcas[marcas['marca'].apply(es_valido)]

# Eliminar duplicados exactos y guardar las limpias
clases_limpias = clases_validas.drop_duplicates(subset=['clase'], keep='first')
marcas_limpias = marcas_validas.drop_duplicates(subset=['marca'], keep='first')

# Generar tabla de eliminados (incluyendo campos vacíos)
def generar_tabla_eliminados(original, limpias, campo):
    eliminados = original[~original.index.isin(limpias.index)].copy()
    eliminados['id_reemplazo'] = eliminados[campo].map(
        lambda x: limpias.loc[limpias[campo] == x, 'id'].iloc[0] if x in limpias[campo].values else 'N/A'
    )
    eliminados['reemplazo'] = eliminados[campo].map(
        lambda x: limpias.loc[limpias[campo] == x, campo].iloc[0] if x in limpias[campo].values else 'N/A'
    )
    return eliminados[['id', campo, 'id_reemplazo', 'reemplazo']]

clases_eliminadas = generar_tabla_eliminados(clases, clases_limpias, 'clase')
marcas_eliminadas = generar_tabla_eliminados(marcas, marcas_limpias, 'marca')

# Crear las tablas codificadas
clases_codificadas = pd.DataFrame({
    'Clases_Equipo': clases_limpias['clase'].sort_values(),
    'Codigo': [f"{i+1:02}" for i in range(len(clases_limpias))]
})

marcas_codificadas = pd.DataFrame({
    'Marcas_Equipo': marcas_limpias['marca'].sort_values(),
    'Codigo': [f"{i+1:02}" for i in range(len(marcas_limpias))]
})

# Guardar en un archivo Excel con múltiples hojas
with pd.ExcelWriter('datos_depurados.xlsx', engine='openpyxl') as writer:
    clases_limpias.to_excel(writer, sheet_name='Clases Limpias', index=False)
    marcas_limpias.to_excel(writer, sheet_name='Marcas Limpias', index=False)
    clases_codificadas.to_excel(writer, sheet_name='Clases Codificadas', index=False)
    marcas_codificadas.to_excel(writer, sheet_name='Marcas Codificadas', index=False)
    clases_eliminadas.to_excel(writer, sheet_name='Clases Eliminadas', index=False)
    marcas_eliminadas.to_excel(writer, sheet_name='Marcas Eliminadas', index=False)

print("Archivo 'datos_depurados.xlsx' generado con las hojas necesarias.")
