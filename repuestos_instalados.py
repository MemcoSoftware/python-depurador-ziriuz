from db import get_db_connection
import pandas as pd

# Consulta SQL
query = """
SELECT 

    actividades.id AS actividad_id,
    MAX(CASE WHEN resultados.id_campo = 2 THEN CAST(resultados.resultado AS UNSIGNED) END) AS RepuestoID,
    MAX(repuestos.nombre) AS Repuesto_Nombre,
    SUM(CASE WHEN resultados.id_campo = 3 THEN CAST(resultados.resultado AS UNSIGNED) ELSE 0 END) AS Cantidad,
    MAX(CASE WHEN resultados.id_campo = 4 THEN CAST(resultados.resultado AS UNSIGNED) END) AS repuesto_valor,
    SUM(CASE WHEN resultados.id_campo = 6 THEN CAST(resultados.resultado AS UNSIGNED) ELSE 0 END) AS repuestos_total,
    actividades.activo AS actividad_activa,
    actividades.id_visita AS actividad_id_visita,
    actividades.id_protocolo,
    actividades.fecha AS actividad_fecha,

    visitas.id AS visita_id,
    visitas.activo AS visita_activa,
    visitas.id_orden AS visita_id_orden,
    visitas.id_estado AS visita_estado,
    visitas.fecha_inicio AS visita_fecha_inicio,
    visitas.ejecutar_sede AS visita_ejecutar_sede,
    visitas.duracion AS visita_duracion,
    visitas.fecha_creacion AS visita_fecha_creacion,
    visitas.fecha_aprobacion AS visita_fecha_aprobacion,
    visitas.observacion_aprobacion AS visita_observacion_aprobacion,
    visitas.fecha_cierre AS visita_fecha_cierre,

    u_responsable.nombre AS nombre_responsable_visita,
    u_creador_visita.nombre AS nombre_creador_visita,
    u_aprobador_visita.nombre AS nombre_aprobador_visita,
    u_cerrador_visita.nombre AS nombre_cerrador_visita,

    ordenes.id AS orden_id,
    ordenes.id_solicitud AS orden_id_solicitud,
    ordenes.id_estado AS orden_estado,
    ordenes.fecha_sub_estado AS orden_fecha_sub_estado,
    ordenes.creacion AS orden_fecha_creacion,
    ordenes.cierre AS orden_fecha_cierre,
    ordenes.observaciones_cierre AS orden_observaciones_cierre,

    u_creador_orden.nombre AS nombre_creador_orden,
    u_entrega_orden.nombre AS nombre_entrega_orden,
    u_cerrador_orden.nombre AS nombre_cerrador_orden,

    solicitudes.id AS solicitud_id,
    solicitudes.creacion AS solicitud_fecha_creacion,
    solicitudes.id_servicio AS solicitud_id_servicio,
    solicitudes.id_estado AS solicitud_estado,
    solicitudes.aviso AS solicitud_aviso,
    solicitudes.cambio_estado AS solicitud_fecha_cambio_estado,
    solicitudes.observacion AS solicitud_observacion,

    u_creador_solicitud.nombre AS nombre_creador_solicitud,
    u_cambiador_solicitud.nombre AS nombre_cambiador_solicitud,

    equipos.id AS equipo_id,
    equipos.id_sede AS equipo_id_sede,
    equipos.id_modelo AS equipo_id_modelo,
    equipos.serie AS equipo_serie,
    equipos.activo_fijo AS equipo_activo_fijo,
    equipos.ubicacion AS equipo_ubicacion,
    equipos.frecuencia AS equipo_frecuencia,
    equipos.mtto AS equipo_mtto,

    modelos.id AS modelo_id,
    modelos.modelo AS modelo_nombre,
    modelos.precio AS modelo_precio,

    clases.id AS clase_id,
    clases.clase AS clase_nombre,

    marcas.id AS marca_id,
    marcas.marca AS marca_nombre,

    tipos.id AS tipo_id,
    tipos.tipo AS tipo_nombre,

    areas.id AS area_id,
    areas.area AS area_nombre,

    sedes.id AS sede_id,
    sedes.nombre AS sede_nombre,
    sedes.direccion AS sede_direccion,
    sedes.telefonos AS sede_telefonos,
    sedes.firman AS sede_firman,
    sedes.sendmail AS sede_sendmail,
    sedes.correo AS sede_correo,


    clientes.id AS cliente_id,
    clientes.nombre AS cliente_nombre,
    clientes.nit AS cliente_nit,
    clientes.direccion AS cliente_direccion,
    clientes.telefonos AS cliente_telefono,


    empresas.id AS empresa_id,
    empresas.nombre AS empresa_nombre,
    empresas.direccion AS empresa_direccion,

    municipios.id AS municipio_id,
    municipios.nombre AS municipio_nombre,


    departamentos.id AS departamento_id,
    departamentos.nombre AS departamento_nombre,


    ordenes_estados.id AS orden_estado_id,
    ordenes_estados.estado AS orden_estado_nombre,


    visitas_estados.id AS visita_estado_id,
    visitas_estados.estado AS visita_estado_nombre

FROM 
    visitas
INNER JOIN 
    actividades ON visitas.id = actividades.id_visita
INNER JOIN 
    resultados ON actividades.id = resultados.id_actividad
LEFT JOIN 
    repuestos ON CAST(resultados.resultado AS UNSIGNED) = repuestos.id
INNER JOIN 
    ordenes ON visitas.id_orden = ordenes.id
INNER JOIN 
    solicitudes ON ordenes.id_solicitud = solicitudes.id
INNER JOIN
    equipos ON solicitudes.id_equipo = equipos.id
INNER JOIN
    modelos ON equipos.id_modelo = modelos.id
INNER JOIN
    clases ON modelos.id_clase = clases.id
INNER JOIN
    marcas ON modelos.id_marca = marcas.id
INNER JOIN
    tipos ON equipos.id_tipo = tipos.id
INNER JOIN
    areas ON equipos.id_area = areas.id
INNER JOIN 
    sedes ON equipos.id_sede = sedes.id
INNER JOIN 
    clientes ON sedes.id_cliente = clientes.id
INNER JOIN 
    empresas ON clientes.id_empresa = empresas.id
INNER JOIN 
    municipios ON sedes.id_municipio = municipios.id
INNER JOIN 
    departamentos ON municipios.id_departamento = departamentos.id
INNER JOIN 
    ordenes_estados ON ordenes.id_estado = ordenes_estados.id
INNER JOIN 
    visitas_estados ON visitas.id_estado = visitas_estados.id
LEFT JOIN 
    usuarios u_responsable ON visitas.id_responsable = u_responsable.id
LEFT JOIN 
    usuarios u_creador_visita ON visitas.id_creador = u_creador_visita.id
LEFT JOIN 
    usuarios u_aprobador_visita ON visitas.id_aprobador = u_aprobador_visita.id
LEFT JOIN 
    usuarios u_cerrador_visita ON visitas.id_cerrador = u_cerrador_visita.id
LEFT JOIN 
    usuarios u_creador_orden ON ordenes.id_creador = u_creador_orden.id
LEFT JOIN 
    usuarios u_entrega_orden ON ordenes.entrega_id = u_entrega_orden.id
LEFT JOIN 
    usuarios u_cerrador_orden ON ordenes.id_cerrador = u_cerrador_orden.id
LEFT JOIN 
    usuarios u_creador_solicitud ON solicitudes.id_creador = u_creador_solicitud.id
LEFT JOIN 
    usuarios u_cambiador_solicitud ON solicitudes.id_cambiador = u_cambiador_solicitud.id
WHERE 
    visitas.id_estado = 3
    AND visitas.activo = 1
    AND actividades.id_protocolo = 2
    AND actividades.activo = 1
    AND ordenes.id_estado = 2
GROUP BY 
    actividades.id;
"""

try:
    # Obtener la conexión
    connection = get_db_connection()
    if connection is None:
        print("No se pudo establecer conexión con la base de datos.")
        exit()

    # Ejecutar la consulta
    cursor = connection.cursor()
    cursor.execute(query)
    resultados = cursor.fetchall()

    # Convertir resultados a DataFrame
    columnas = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(resultados, columns=columnas)

    # Depuración de clases
    clases_df = (
        df[['clase_nombre']]
        .drop_duplicates()  # Eliminar duplicados
        .dropna()  # Eliminar valores nulos
        .assign(clase_nombre=lambda x: x['clase_nombre'].str.strip().str.upper())  # Quitar espacios y convertir a mayúsculas
        .sort_values(by='clase_nombre')  # Ordenar alfabéticamente
    )
    clases_df = clases_df.rename(columns={'clase_nombre': 'Clase'})

    # Depuración de marcas
    marcas_df = (
        df[['marca_nombre']]
        .drop_duplicates()
        .dropna()
        .assign(marca_nombre=lambda x: x['marca_nombre'].str.strip().str.upper())
        .sort_values(by='marca_nombre')
    )
    marcas_df = marcas_df.rename(columns={'marca_nombre': 'Marca'})

    # Depuración de modelos
    modelos_df = (
        df[['clase_nombre', 'modelo_nombre', 'marca_nombre']]
        .drop_duplicates()
        .dropna(subset=['clase_nombre', 'modelo_nombre', 'marca_nombre'])  # Eliminar nulos en las 3 columnas
        .assign(
            clase_nombre=lambda x: x['clase_nombre'].str.strip().str.upper(),
            modelo_nombre=lambda x: x['modelo_nombre'].str.strip().str.upper(),
            marca_nombre=lambda x: x['marca_nombre'].str.strip().str.upper(),
        )
        .sort_values(by=['clase_nombre', 'modelo_nombre', 'marca_nombre'])  # Ordenar por las 3 columnas
    )
    modelos_df = modelos_df.rename(columns={
        'clase_nombre': 'Clase',
        'modelo_nombre': 'Modelo',
        'marca_nombre': 'Marca'
    })

    # Depuración de repuestos codificados con generación de Código Repuesto
    repuestos_codificados_df = (
        df[['clase_nombre', 'modelo_nombre', 'marca_nombre', 'Repuesto_Nombre']]
        .drop_duplicates()  # Eliminar duplicados
        .dropna()  # Eliminar valores nulos
        .assign(
            clase_nombre=lambda x: x['clase_nombre'].str.strip().str.upper(),
            modelo_nombre=lambda x: x['modelo_nombre'].str.strip().str.upper(),
            marca_nombre=lambda x: x['marca_nombre'].str.strip().str.upper(),
            Repuesto_Nombre=lambda x: x['Repuesto_Nombre'].str.strip().str.upper(),
        )
        .sort_values(by=['clase_nombre', 'modelo_nombre', 'marca_nombre', 'Repuesto_Nombre'])  # Ordenar alfabéticamente
    )
    repuestos_codificados_df = repuestos_codificados_df.rename(columns={
        'clase_nombre': 'Clase',
        'modelo_nombre': 'Modelo',
        'marca_nombre': 'Marca',
        'Repuesto_Nombre': 'Repuesto'
    })

    # Generar Código Repuesto
    repuestos_codificados_df['Código Repuesto'] = (
        repuestos_codificados_df['Clase'].str[0].fillna('X') +  # Inicial de Clase
        repuestos_codificados_df['Modelo'].str[0].fillna('X') +  # Inicial de Modelo
        repuestos_codificados_df['Marca'].str[0].fillna('X') +  # Inicial de Marca
        (repuestos_codificados_df.reset_index().index + 1).astype(str).str.zfill(5)  # Índice como número consecutivo con 5 dígitos
    )

    # Exportar a Excel
    salida_excel = 'repuestos_depurados.xlsx'
    with pd.ExcelWriter(salida_excel, engine='openpyxl') as writer:
        # Hoja principal con los resultados de la consulta
        df.to_excel(writer, sheet_name='Repuestos Instalados', index=False)
        # Hoja de clases
        clases_df.to_excel(writer, sheet_name='Clases', index=False)
        # Hoja de marcas
        marcas_df.to_excel(writer, sheet_name='Marcas', index=False)
        # Hoja de modelos
        modelos_df.to_excel(writer, sheet_name='Modelos', index=False)
        # Hoja de repuestos codificados
        repuestos_codificados_df.to_excel(writer, sheet_name='Repuestos_Codificados', index=False)

    print(f"Resultados exportados a {salida_excel}")

    # Cerrar cursor y conexión
    cursor.close()
    connection.close()

except Exception as e:
    print(f"Error al ejecutar la consulta: {e}")
