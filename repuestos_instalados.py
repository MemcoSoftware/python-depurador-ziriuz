from db import get_db_connection
import pandas as pd

# Consulta SQL
query = """
SELECT 
    a.actividad_id,
    a.RepuestoID,
    r.nombre AS Repuesto_Nombre,
    a.Cantidad,
    a.repuesto_valor,
    a.repuestos_total,
    a.RepuestoActivo,
    a.actividad_activa,
    a.actividad_id_visita,
    a.id_protocolo,
    a.actividad_fecha,

    a.visita_id,
    a.visita_activa,
    a.visita_id_orden,
    a.visita_estado,
    a.visita_fecha_inicio,
    a.visita_ejecutar_sede,
    a.visita_duracion,
    a.visita_fecha_creacion,
    a.visita_fecha_aprobacion,
    a.visita_observacion_aprobacion,
    a.visita_fecha_cierre,

    a.nombre_responsable_visita,
    a.nombre_creador_visita,
    a.nombre_aprobador_visita,
    a.nombre_cerrador_visita,

    a.orden_id,
    a.orden_id_solicitud,
    a.orden_estado,
    a.orden_fecha_sub_estado,
    a.orden_fecha_creacion,
    a.orden_fecha_cierre,
    a.orden_observaciones_cierre,

    a.nombre_creador_orden,
    a.nombre_entrega_orden,
    a.nombre_cerrador_orden,

    a.solicitud_id,
    a.solicitud_fecha_creacion,
    a.solicitud_id_servicio,
    a.solicitud_estado,
    a.solicitud_aviso,
    a.solicitud_fecha_cambio_estado,
    a.solicitud_observacion,

    a.nombre_creador_solicitud,
    a.nombre_cambiador_solicitud,

    a.equipo_id,
    a.equipo_id_sede,
    a.equipo_id_modelo,
    a.equipo_serie,
    a.equipo_activo_fijo,
    a.equipo_ubicacion,
    a.equipo_frecuencia,
    a.equipo_mtto,

    a.modelo_id,
    a.modelo_nombre,
    a.modelo_precio,

    a.clase_id,
    a.clase_nombre,

    a.marca_id,
    a.marca_nombre,

    a.tipo_id,
    a.tipo_nombre,

    a.area_id,
    a.area_nombre,

    a.sede_id,
    a.sede_nombre,
    a.sede_direccion,
    a.sede_telefonos,
    a.sede_firman,
    a.sede_sendmail,
    a.sede_correo,

    a.cliente_id,
    a.cliente_nombre,
    a.cliente_nit,
    a.cliente_direccion,
    a.cliente_telefono,

    a.empresa_id,
    a.empresa_nombre,
    a.empresa_direccion,

    a.municipio_id,
    a.municipio_nombre,

    a.departamento_id,
    a.departamento_nombre,

    a.orden_estado_id,
    a.orden_estado_nombre,

    a.visita_estado_id,
    a.visita_estado_nombre
FROM (
    SELECT 
        actividades.id AS actividad_id,
        MAX(CASE WHEN resultados.id_campo = 2 THEN CAST(resultados.resultado AS UNSIGNED) END) AS RepuestoID,
        SUM(CASE WHEN resultados.id_campo = 3 THEN CAST(resultados.resultado AS UNSIGNED) ELSE 0 END) AS Cantidad,
        MAX(CASE WHEN resultados.id_campo = 4 THEN CAST(resultados.resultado AS UNSIGNED) END) AS repuesto_valor,
        SUM(CASE WHEN resultados.id_campo = 6 THEN CAST(resultados.resultado AS UNSIGNED) ELSE 0 END) AS repuestos_total,
        MAX(CASE WHEN resultados.id_campo = 2 THEN repuestos.activo END) AS RepuestoActivo,
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
    INNER JOIN actividades ON visitas.id = actividades.id_visita
    INNER JOIN resultados ON actividades.id = resultados.id_actividad
    LEFT JOIN repuestos ON CAST(resultados.resultado AS UNSIGNED) = repuestos.id
    INNER JOIN ordenes ON visitas.id_orden = ordenes.id
    INNER JOIN solicitudes ON ordenes.id_solicitud = solicitudes.id
    INNER JOIN equipos ON solicitudes.id_equipo = equipos.id
    INNER JOIN modelos ON equipos.id_modelo = modelos.id
    INNER JOIN clases ON modelos.id_clase = clases.id
    INNER JOIN marcas ON modelos.id_marca = marcas.id
    INNER JOIN tipos ON equipos.id_tipo = tipos.id
    INNER JOIN areas ON equipos.id_area = areas.id
    INNER JOIN sedes ON equipos.id_sede = sedes.id
    INNER JOIN clientes ON sedes.id_cliente = clientes.id
    INNER JOIN empresas ON clientes.id_empresa = empresas.id
    INNER JOIN municipios ON sedes.id_municipio = municipios.id
    INNER JOIN departamentos ON municipios.id_departamento = departamentos.id
    INNER JOIN ordenes_estados ON ordenes.id_estado = ordenes_estados.id
    INNER JOIN visitas_estados ON visitas.id_estado = visitas_estados.id
    LEFT JOIN usuarios u_responsable ON visitas.id_responsable = u_responsable.id
    LEFT JOIN usuarios u_creador_visita ON visitas.id_creador = u_creador_visita.id
    LEFT JOIN usuarios u_aprobador_visita ON visitas.id_aprobador = u_aprobador_visita.id
    LEFT JOIN usuarios u_cerrador_visita ON visitas.id_cerrador = u_cerrador_visita.id
    LEFT JOIN usuarios u_creador_orden ON ordenes.id_creador = u_creador_orden.id
    LEFT JOIN usuarios u_entrega_orden ON ordenes.entrega_id = u_entrega_orden.id
    LEFT JOIN usuarios u_cerrador_orden ON ordenes.id_cerrador = u_cerrador_orden.id
    LEFT JOIN usuarios u_creador_solicitud ON solicitudes.id_creador = u_creador_solicitud.id
    LEFT JOIN usuarios u_cambiador_solicitud ON solicitudes.id_cambiador = u_cambiador_solicitud.id
    WHERE 
        visitas.id_estado = 3
        AND visitas.activo = 1
        AND actividades.id_protocolo = 2
        AND actividades.activo = 1
        AND ordenes.id_estado = 2
    GROUP BY 
        actividades.id
) a
LEFT JOIN repuestos r ON a.RepuestoID = r.id;
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

    # Crear la nueva hoja Repuestos_Instalados_SURA_2024
    repuestos_sura_df = (
        df[
            (df['cliente_nombre'].str.contains(r'\.\.', regex=True, na=False)) &  # Filtrar por los clientes especificados
            (df['orden_fecha_cierre'].astype(str).str.startswith('2024'))  # Convertir a string y filtrar por el año 2024
        ][[
            'cliente_id', 'cliente_nombre', 'clase_nombre', 'modelo_nombre', 'marca_nombre', 'RepuestoID',
            'Repuesto_Nombre', 'RepuestoActivo', 'repuesto_valor'
        ]]
        .drop_duplicates()
        .dropna()
        .assign(
            cliente_id=lambda x: x['cliente_id'].astype(int),
            cliente_nombre=lambda x: x['cliente_nombre'].str.strip().str.upper(),
            clase_nombre=lambda x: x['clase_nombre'].str.strip().str.upper(),
            modelo_nombre=lambda x: x['modelo_nombre'].str.strip().str.upper(),
            marca_nombre=lambda x: x['marca_nombre'].str.strip().str.upper(),
            Repuesto_Nombre=lambda x: x['Repuesto_Nombre'].str.strip().str.upper(),
            # Convertir 'Valor' a formato de moneda COP
            repuesto_valor=lambda x: x['repuesto_valor'].apply(lambda v: f"${v:,.0f} COP" if pd.notnull(v) else None),
        )
    )

    # Generar Código Repuesto con prefijo "S-"
    repuestos_sura_df = repuestos_sura_df.assign(
        Código_Repuesto=lambda x: (
            "S-" +  # Prefijo fijo "S-"
            x['clase_nombre'].str[0].fillna('X') +  # Inicial de Clase
            x['modelo_nombre'].str[0].fillna('X') +  # Inicial de Modelo
            x['marca_nombre'].str[0].fillna('X') +  # Inicial de Marca
            (x.reset_index().index + 1).astype(str).str.zfill(5)  # Índice consecutivo con 5 dígitos
        )
    )

    # Renombrar las columnas para que coincidan con los nombres requeridos
    repuestos_sura_df = repuestos_sura_df.rename(columns={
        'cliente_id': 'ID CLIENTE',
        'cliente_nombre': 'CLIENTE',
        'clase_nombre': 'Clase',
        'modelo_nombre': 'Modelo',
        'marca_nombre': 'Marca',
        'RepuestoID': 'ID Repuesto',
        'Repuesto_Nombre': 'Repuesto',
        'RepuestoActivo': 'ACTIVO',
        'repuesto_valor': 'Valor',
        'Código_Repuesto': 'Código Repuesto'
    })

    # Crear la nueva hoja Repuestos_Instalados_Inactivos_SURA_2024
    repuestos_inactivos_sura_df = (
        df[
            (df['cliente_nombre'].str.contains(r'\.\.', regex=True, na=False)) &  # Filtrar por los clientes especificados
            (df['orden_fecha_cierre'].astype(str).str.startswith('2024')) &  # Convertir a string y filtrar por el año 2024
            (df['RepuestoActivo'] == 0)  # Filtrar por repuestos inactivos (Activo = 0)
        ][[
            'cliente_id', 'cliente_nombre', 'clase_nombre', 'modelo_nombre', 'marca_nombre',
            'Repuesto_Nombre', 'RepuestoActivo', 'repuesto_valor'
        ]]
        .drop_duplicates()
        .dropna()
        .assign(
            cliente_id=lambda x: x['cliente_id'].astype(int),
            cliente_nombre=lambda x: x['cliente_nombre'].str.strip().str.upper(),
            clase_nombre=lambda x: x['clase_nombre'].str.strip().str.upper(),
            modelo_nombre=lambda x: x['modelo_nombre'].str.strip().str.upper(),
            marca_nombre=lambda x: x['marca_nombre'].str.strip().str.upper(),
            Repuesto_Nombre=lambda x: x['Repuesto_Nombre'].str.strip().str.upper(),
            # Convertir 'Valor' a formato de moneda COP
            repuesto_valor=lambda x: x['repuesto_valor'].apply(lambda v: f"${v:,.0f} COP" if pd.notnull(v) else None),
        )
        .sort_values(by=['cliente_id', 'clase_nombre', 'modelo_nombre', 'marca_nombre', 'Repuesto_Nombre'])
    )

    # Renombrar las columnas para que coincidan con los nombres requeridos
    repuestos_inactivos_sura_df = repuestos_inactivos_sura_df.rename(columns={
        'cliente_id': 'ID CLIENTE',
        'cliente_nombre': 'CLIENTE',
        'clase_nombre': 'Clase',
        'modelo_nombre': 'Modelo',
        'marca_nombre': 'Marca',
        'Repuesto_Nombre': 'Repuesto',
        'RepuestoActivo': 'ACTIVO',
        'repuesto_valor': 'Valor',
    })

    # Crear la nueva hoja RepDesactivarSURA
    # Obtener los repuestos que ya están en InstaladosSURA2024 (Año 2024)
    repuestos_2024 = set(
        repuestos_sura_df[['Clase', 'Modelo', 'Marca', 'Repuesto']].apply(tuple, axis=1)
    )

    # Filtrar los repuestos que NO están en InstaladosSURA2024 y que tienen otro año distinto a 2024
    rep_desactivar_sura_df = (
        df[
            (df['cliente_nombre'].str.contains(r'\.\.', regex=True, na=False)) &  # Filtrar clientes con ".." en el nombre
            (~df['orden_fecha_cierre'].astype(str).str.startswith('2024'))  # Años distintos de 2024
        ][[
            'cliente_id', 'cliente_nombre', 'clase_nombre', 'modelo_nombre', 'marca_nombre',
            'RepuestoID', 'Repuesto_Nombre', 'RepuestoActivo', 'repuesto_valor'
        ]]
        .drop_duplicates()
        .dropna()
        .assign(
            cliente_id=lambda x: x['cliente_id'].astype(int),
            cliente_nombre=lambda x: x['cliente_nombre'].str.strip().str.upper(),
            clase_nombre=lambda x: x['clase_nombre'].str.strip().str.upper(),
            modelo_nombre=lambda x: x['modelo_nombre'].str.strip().str.upper(),
            marca_nombre=lambda x: x['marca_nombre'].str.strip().str.upper(),
            Repuesto_Nombre=lambda x: x['Repuesto_Nombre'].str.strip().str.upper(),
            # Convertir 'Valor' a formato de moneda COP
            repuesto_valor=lambda x: x['repuesto_valor'].apply(lambda v: f"${v:,.0f} COP" if pd.notnull(v) else None),
        )
    )

    # Eliminar los repuestos que ya existen en InstaladosSURA2024
    rep_desactivar_sura_df = rep_desactivar_sura_df[
        ~rep_desactivar_sura_df[['clase_nombre', 'modelo_nombre', 'marca_nombre', 'Repuesto_Nombre']].apply(tuple, axis=1).isin(repuestos_2024)
    ]

    # Renombrar las columnas para que coincidan con los nombres requeridos
    rep_desactivar_sura_df = rep_desactivar_sura_df.rename(columns={
        'cliente_id': 'ID CLIENTE',
        'cliente_nombre': 'CLIENTE',
        'clase_nombre': 'Clase',
        'modelo_nombre': 'Modelo',
        'marca_nombre': 'Marca',
        'RepuestoID': 'ID Repuesto',  # 🔹 Nueva columna añadida
        'Repuesto_Nombre': 'Repuesto',
        'RepuestoActivo': 'ACTIVO',
        'repuesto_valor': 'Valor'
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
        df.to_excel(writer, sheet_name='RepuestosInstalados', index=False)  # Cambiar nombre
        # Hoja de clases
        clases_df.to_excel(writer, sheet_name='Clases', index=False)
        # Hoja de marcas
        marcas_df.to_excel(writer, sheet_name='Marcas', index=False)
        # Hoja de modelos
        modelos_df.to_excel(writer, sheet_name='Modelos', index=False)
        # Hoja de repuestos codificados
        repuestos_codificados_df.to_excel(writer, sheet_name='RepuestosCodif', index=False)  # Cambiar nombre
        # Exportar la nueva hoja Repuestos_Instalados_SURA_2024
        repuestos_sura_df.to_excel(writer, sheet_name='InstaladosSURA2024', index=False)  # Cambiar nombre
        # Exportar la nueva hoja Repuestos_Instalados_Inactivos_SURA_2024
        repuestos_inactivos_sura_df.to_excel(writer, sheet_name='InactivosSURA2024', index=False)  # Cambiar nombre
        #Exportar la nueva hoja RepDesactivarSURA
        rep_desactivar_sura_df.to_excel(writer, sheet_name='RepDesactivarSURA', index=False)  # Nueva hoja agregada


    print(f"Resultados exportados a {salida_excel}")

    # Cerrar cursor y conexión
    cursor.close()
    connection.close()

except Exception as e:
    print(f"Error al ejecutar la consulta: {e}")
 