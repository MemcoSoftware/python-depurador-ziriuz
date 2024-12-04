import pandas as pd
import os
import unicodedata

# Ruta del archivo CSV
ruta_modelos = r'C:\Users\B4N3D or Banned\Documents\MEMCO\Ziriuz1\DEPURACIÓN ZIRIUZ\MODELOS EQUIPOS\modelos.csv'

# Verificar si el archivo existe
if not os.path.exists(ruta_modelos):
    print(f"Error: El archivo '{ruta_modelos}' no existe.")
    exit()

# Cargar el archivo CSV
try:
    modelos = pd.read_csv(ruta_modelos, encoding='utf-8', sep=',')
except Exception as e:
    print(f"Error al cargar el archivo CSV: {e}")
    exit()

# Convertir columnas relevantes a enteros
modelos['id_clase'] = pd.to_numeric(modelos['id_clase'], errors='coerce').fillna(0).astype(int)
modelos['id_marca'] = pd.to_numeric(modelos['id_marca'], errors='coerce').fillna(0).astype(int)

# Normalizar texto: quitar tildes, convertir a minúsculas
def normalizar_texto(texto):
    if pd.isnull(texto):
        return ""
    return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8').strip().lower()

modelos['clase'] = modelos['clase'].astype(str).apply(normalizar_texto)
modelos['marca'] = modelos['marca'].astype(str).apply(normalizar_texto)

# Filtrar y remover campos vacíos o que contienen solo espacios/puntos
def es_valido(valor):
    return bool(valor.strip().replace('.', ''))

modelos_validos = modelos[
    modelos['clase'].apply(es_valido) & modelos['marca'].apply(es_valido)
]

# Separar las tablas de clases y marcas para su depuración
clases = modelos[['id_clase', 'clase']].drop_duplicates().rename(columns={'id_clase': 'id'})
marcas = modelos[['id_marca', 'marca']].drop_duplicates().rename(columns={'id_marca': 'id'})

# Eliminar duplicados exactos y guardar las limpias
clases_limpias = clases.drop_duplicates(subset=['clase'], keep='first')
marcas_limpias = marcas.drop_duplicates(subset=['marca'], keep='first')

# Generar tabla de eliminados
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

# Guardar en un archivo Excel
salida_excel = r'C:\Users\B4N3D or Banned\Documents\MEMCO\Ziriuz1\DEPURACIÓN ZIRIUZ\modelos_depurados.xlsx'

with pd.ExcelWriter(salida_excel, engine='openpyxl') as writer:
    clases_limpias.to_excel(writer, sheet_name='Clases Limpias', index=False)
    marcas_limpias.to_excel(writer, sheet_name='Marcas Limpias', index=False)
    clases_codificadas.to_excel(writer, sheet_name='Clases Codificadas', index=False)
    marcas_codificadas.to_excel(writer, sheet_name='Marcas Codificadas', index=False)
    clases_eliminadas.to_excel(writer, sheet_name='Clases Eliminadas', index=False)
    marcas_eliminadas.to_excel(writer, sheet_name='Marcas Eliminadas', index=False)

print(f"Archivo '{salida_excel}' generado con las hojas necesarias.")
