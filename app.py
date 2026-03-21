import os
import re
import unicodedata
from datetime import date, datetime
from io import BytesIO

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

st.set_page_config(page_title="Gestión de jugadores", layout="wide")

# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================
APP_CLIENTE_ID = st.secrets.get("CLIENTE_ID", os.getenv("CLIENTE_ID", "default"))
COLUMNAS_REQUERIDAS = ["Nombre", "Apellido", "DNI", "Edad"]
COLUMNAS_OPCIONALES = ["Observaciones", "Activo"]
COLUMNAS_ADMITIDAS = COLUMNAS_REQUERIDAS + COLUMNAS_OPCIONALES
MAPA_ENCABEZADOS = {
    "nombre": "Nombre",
    "nombres": "Nombre",
    "apellido": "Apellido",
    "apellidos": "Apellido",
    "dni": "DNI",
    "documento": "DNI",
    "numerodni": "DNI",
    "edad": "Edad",
    "edades": "Edad",
    "observacion": "Observaciones",
    "observaciones": "Observaciones",
    "obs": "Observaciones",
    "activo": "Activo",
    "activos": "Activo",
    "estado": "Activo",
}


# =========================================================
# UTILIDADES BÁSICAS
# =========================================================
def normalizar_dni(valor) -> str:
    if pd.isna(valor):
        return ""
    return "".join(ch for ch in str(valor) if ch.isdigit())



def limpiar_texto(valor) -> str:
    if pd.isna(valor):
        return ""
    return str(valor).strip()



def hoy_str() -> str:
    return date.today().isoformat()



def ahora_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")



def mes_actual_str() -> str:
    hoy = date.today()
    return f"{hoy.year}-{hoy.month:02d}"



def es_mes_valido(valor: str) -> bool:
    if not isinstance(valor, str):
        return False
    valor = valor.strip()
    if not re.fullmatch(r"\d{4}-\d{2}", valor):
        return False
    anio, mes = map(int, valor.split("-"))
    return 1 <= mes <= 12 and 2000 <= anio <= 2100



def valor_activo_desde_excel(valor) -> int:
    if pd.isna(valor):
        return 1
    texto = limpiar_texto(valor).lower()
    if texto in {"0", "no", "false", "falso", "inactivo", "baja"}:
        return 0
    return 1



def normalizar_encabezado(columna: str) -> str:
    texto = limpiar_texto(columna).lower()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = re.sub(r"[^a-z0-9]", "", texto)
    return texto



def label_jugador(row: pd.Series) -> str:
    apellido = limpiar_texto(row.get("apellido", ""))
    nombre = limpiar_texto(row.get("nombre", ""))
    dni = limpiar_texto(row.get("dni", ""))
    return f"{apellido}, {nombre} - DNI {dni}"



def truncar_texto(valor: str, largo: int = 50) -> str:
    texto = limpiar_texto(valor)
    if len(texto) <= largo:
        return texto
    return texto[: largo - 3] + "..."


# =========================================================
# BASE DE DATOS
# =========================================================
def obtener_database_url() -> str:
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]
    env_value = os.getenv("DATABASE_URL", "")
    if env_value:
        return env_value
    raise RuntimeError(
        "Falta DATABASE_URL. Cargá la URI del Session Pooler de Supabase en secrets.toml o en los Secrets de Streamlit."
    )


@st.cache_resource
def get_engine():
    return create_engine(
        obtener_database_url(),
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True,
    )



def ejecutar_select(query: str, params=None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(query), conn, params=params or {})



def ejecutar_accion(query: str, params=None):
    with get_engine().begin() as conn:
        return conn.execute(text(query), params or {})


# =========================================================
# INICIALIZACIÓN Y SEGURIDAD BÁSICA DE ESTRUCTURA
# =========================================================
def verificar_tablas_requeridas() -> list[str]:
    query = """
    select table_name
    from information_schema.tables
    where table_schema = 'public'
      and table_name in ('clientes', 'configuracion_app', 'jugadores', 'pagos', 'auditoria')
    """
    df = ejecutar_select(query)
    existentes = set(df["table_name"].tolist()) if not df.empty else set()
    requeridas = {"clientes", "configuracion_app", "jugadores", "pagos", "auditoria"}
    return sorted(list(requeridas - existentes))



def asegurar_cliente_base():
    ejecutar_accion(
        """
        insert into public.clientes (id, nombre, activo)
        values (:id, :nombre, 1)
        on conflict (id) do nothing
        """,
        {"id": APP_CLIENTE_ID, "nombre": "Escuelita"},
    )



def inicializar_configuracion():
    ejecutar_accion(
        """
        insert into public.configuracion_app (clave, valor)
        values (:clave, :valor)
        on conflict (clave) do nothing
        """,
        {"clave": "organizacion_nombre", "valor": "Escuelita"},
    )
    ejecutar_accion(
        """
        insert into public.configuracion_app (clave, valor)
        values (:clave, :valor)
        on conflict (clave) do nothing
        """,
        {"clave": "moneda", "valor": "ARS"},
    )


# =========================================================
# CONFIGURACIÓN Y AUDITORÍA
# =========================================================
def obtener_config(clave: str, default: str = "") -> str:
    df = ejecutar_select(
        "select valor from public.configuracion_app where clave = :clave",
        {"clave": clave},
    )
    if df.empty:
        return default
    return str(df.iloc[0]["valor"])



def guardar_config(clave: str, valor: str):
    ejecutar_accion(
        """
        insert into public.configuracion_app (clave, valor)
        values (:clave, :valor)
        on conflict (clave) do update set valor = excluded.valor
        """,
        {"clave": clave, "valor": valor},
    )



def registrar_auditoria(entidad: str, accion: str, detalle: str = "", entidad_id=None):
    ejecutar_accion(
        """
        insert into public.auditoria (cliente_id, entidad, entidad_id, accion, detalle, fecha_evento)
        values (:cliente_id, :entidad, :entidad_id, :accion, :detalle, :fecha_evento)
        """,
        {
            "cliente_id": APP_CLIENTE_ID,
            "entidad": entidad,
            "entidad_id": entidad_id,
            "accion": accion,
            "detalle": detalle,
            "fecha_evento": ahora_str(),
        },
    )


# =========================================================
# CONSULTAS PRINCIPALES
# =========================================================
def obtener_jugadores(incluir_inactivos: bool = True) -> pd.DataFrame:
    query = """
    select id, cliente_id, nombre, apellido, dni, edad, activo, observaciones, fecha_alta, fecha_actualizacion
    from public.jugadores
    where cliente_id = :cliente_id
    """
    if not incluir_inactivos:
        query += " and activo = 1"
    query += " order by apellido, nombre"
    return ejecutar_select(query, {"cliente_id": APP_CLIENTE_ID})



def obtener_jugador_por_dni(dni: str):
    df = ejecutar_select(
        """
        select id, nombre, apellido, dni, edad, activo, observaciones
        from public.jugadores
        where cliente_id = :cliente_id and dni = :dni
        """,
        {"cliente_id": APP_CLIENTE_ID, "dni": normalizar_dni(dni)},
    )
    if df.empty:
        return None
    row = df.iloc[0]
    return [row["id"], row["nombre"], row["apellido"], row["dni"], row["edad"], row["activo"], row["observaciones"]]



def obtener_resumen_estado() -> pd.DataFrame:
    query = """
    select j.id,
           j.nombre,
           j.apellido,
           j.dni,
           j.edad,
           j.activo,
           j.observaciones,
           max(p.mes_correspondiente) as ultimo_mes_pagado,
           max(p.fecha_pago) as ultima_fecha_pago
    from public.jugadores j
    left join public.pagos p
        on p.jugador_id = j.id
       and p.cliente_id = j.cliente_id
    where j.cliente_id = :cliente_id
    group by j.id, j.nombre, j.apellido, j.dni, j.edad, j.activo, j.observaciones
    order by j.apellido, j.nombre
    """
    return ejecutar_select(query, {"cliente_id": APP_CLIENTE_ID})



def obtener_pagos(mes: str | None = None) -> pd.DataFrame:
    query = """
    select p.id,
           p.jugador_id,
           j.nombre || case when trim(j.apellido) <> '' then ' ' || j.apellido else '' end as jugador,
           j.apellido,
           j.nombre,
           j.dni,
           p.fecha_pago,
           p.monto,
           p.mes_correspondiente,
           coalesce(p.observaciones, '') as observaciones,
           p.fecha_creacion
    from public.pagos p
    inner join public.jugadores j on p.jugador_id = j.id and p.cliente_id = j.cliente_id
    where p.cliente_id = :cliente_id
    """
    params = {"cliente_id": APP_CLIENTE_ID}
    if mes:
        query += " and p.mes_correspondiente = :mes"
        params["mes"] = mes
    query += " order by p.fecha_pago desc, p.id desc"
    return ejecutar_select(query, params)



def obtener_auditoria(limite: int = 100) -> pd.DataFrame:
    query = """
    select id, entidad, entidad_id, accion, detalle, fecha_evento
    from public.auditoria
    where cliente_id = :cliente_id
    order by id desc
    limit :limite
    """
    return ejecutar_select(query, {"cliente_id": APP_CLIENTE_ID, "limite": limite})



def obtener_jugadores_deben_mes(mes: str) -> pd.DataFrame:
    query = """
    select j.id, j.nombre, j.apellido, j.dni, j.edad, j.activo, j.observaciones
    from public.jugadores j
    where j.cliente_id = :cliente_id
      and j.activo = 1
      and not exists (
          select 1
          from public.pagos p
          where p.cliente_id = j.cliente_id
            and p.jugador_id = j.id
            and p.mes_correspondiente = :mes
      )
    order by j.apellido, j.nombre
    """
    return ejecutar_select(query, {"cliente_id": APP_CLIENTE_ID, "mes": mes})


# =========================================================
# OPERACIONES DE NEGOCIO
# =========================================================
def agregar_jugador(nombre, apellido, dni, edad, observaciones, activo: int = 1):
    result = ejecutar_accion(
        """
        insert into public.jugadores (
            cliente_id, nombre, apellido, dni, edad, activo, observaciones, fecha_alta, fecha_actualizacion
        )
        values (
            :cliente_id, :nombre, :apellido, :dni, :edad, :activo, :observaciones, :fecha_alta, :fecha_actualizacion
        )
        returning id
        """,
        {
            "cliente_id": APP_CLIENTE_ID,
            "nombre": limpiar_texto(nombre),
            "apellido": limpiar_texto(apellido),
            "dni": normalizar_dni(dni),
            "edad": int(edad) if edad not in [None, ""] and pd.notna(edad) else None,
            "activo": int(activo),
            "observaciones": limpiar_texto(observaciones),
            "fecha_alta": hoy_str(),
            "fecha_actualizacion": ahora_str(),
        },
    )
    jugador_id = result.scalar_one()
    registrar_auditoria(
        entidad="jugadores",
        entidad_id=jugador_id,
        accion="alta",
        detalle=f"Alta de jugador DNI {normalizar_dni(dni)}",
    )
    return jugador_id



def actualizar_jugador(jugador_id, nombre, apellido, dni, edad, activo, observaciones):
    ejecutar_accion(
        """
        update public.jugadores
        set nombre = :nombre,
            apellido = :apellido,
            dni = :dni,
            edad = :edad,
            activo = :activo,
            observaciones = :observaciones,
            fecha_actualizacion = :fecha_actualizacion
        where id = :jugador_id and cliente_id = :cliente_id
        """,
        {
            "nombre": limpiar_texto(nombre),
            "apellido": limpiar_texto(apellido),
            "dni": normalizar_dni(dni),
            "edad": int(edad) if edad not in [None, ""] and pd.notna(edad) else None,
            "activo": int(activo),
            "observaciones": limpiar_texto(observaciones),
            "fecha_actualizacion": ahora_str(),
            "jugador_id": jugador_id,
            "cliente_id": APP_CLIENTE_ID,
        },
    )
    registrar_auditoria(
        entidad="jugadores",
        entidad_id=jugador_id,
        accion="edicion",
        detalle=f"Actualización de jugador DNI {normalizar_dni(dni)}",
    )



def cambiar_estado_jugador(jugador_id: int, activo: int):
    ejecutar_accion(
        """
        update public.jugadores
        set activo = :activo, fecha_actualizacion = :fecha_actualizacion
        where id = :jugador_id and cliente_id = :cliente_id
        """,
        {
            "activo": int(activo),
            "fecha_actualizacion": ahora_str(),
            "jugador_id": jugador_id,
            "cliente_id": APP_CLIENTE_ID,
        },
    )
    registrar_auditoria(
        entidad="jugadores",
        entidad_id=jugador_id,
        accion="reactivacion" if int(activo) == 1 else "baja_logica",
        detalle="Jugador marcado como activo" if int(activo) == 1 else "Jugador marcado como inactivo",
    )



def registrar_pago(jugador_id, fecha_pago, monto, mes_correspondiente, observaciones):
    if not es_mes_valido(mes_correspondiente):
        raise ValueError("El mes correspondiente debe estar en formato YYYY-MM.")

    result = ejecutar_accion(
        """
        insert into public.pagos (cliente_id, jugador_id, fecha_pago, monto, mes_correspondiente, observaciones, fecha_creacion)
        values (:cliente_id, :jugador_id, :fecha_pago, :monto, :mes_correspondiente, :observaciones, :fecha_creacion)
        returning id
        """,
        {
            "cliente_id": APP_CLIENTE_ID,
            "jugador_id": jugador_id,
            "fecha_pago": str(fecha_pago),
            "monto": float(monto),
            "mes_correspondiente": mes_correspondiente.strip(),
            "observaciones": limpiar_texto(observaciones),
            "fecha_creacion": ahora_str(),
        },
    )
    pago_id = result.scalar_one()
    registrar_auditoria(
        entidad="pagos",
        entidad_id=pago_id,
        accion="alta",
        detalle=f"Pago registrado para jugador {jugador_id} - mes {mes_correspondiente}",
    )
    return pago_id



def actualizar_pago(pago_id: int, monto, observaciones: str):
    ejecutar_accion(
        """
        update public.pagos
        set monto = :monto,
            observaciones = :observaciones
        where id = :pago_id and cliente_id = :cliente_id
        """,
        {
            "monto": float(monto),
            "observaciones": limpiar_texto(observaciones),
            "pago_id": int(pago_id),
            "cliente_id": APP_CLIENTE_ID,
        },
    )
    registrar_auditoria(
        entidad="pagos",
        entidad_id=int(pago_id),
        accion="edicion",
        detalle="Actualización de monto/observaciones en historial de pagos",
    )



def borrar_base_operativa(conservar_auditoria: bool = True):
    jugadores_df = obtener_jugadores(incluir_inactivos=True)
    pagos_df = obtener_pagos()
    total_jugadores = len(jugadores_df)
    total_pagos = len(pagos_df)

    ejecutar_accion(
        "delete from public.pagos where cliente_id = :cliente_id",
        {"cliente_id": APP_CLIENTE_ID},
    )
    ejecutar_accion(
        "delete from public.jugadores where cliente_id = :cliente_id",
        {"cliente_id": APP_CLIENTE_ID},
    )

    if not conservar_auditoria:
        ejecutar_accion(
            "delete from public.auditoria where cliente_id = :cliente_id",
            {"cliente_id": APP_CLIENTE_ID},
        )
    else:
        registrar_auditoria(
            entidad="mantenimiento",
            accion="borrado_base_operativa",
            detalle=f"Se eliminaron {total_jugadores} jugadores y {total_pagos} pagos.",
        )
    return total_jugadores, total_pagos


# =========================================================
# IMPORTACIÓN Y EXPORTACIÓN
# =========================================================
def leer_archivo_carga(archivo) -> pd.DataFrame:
    nombre = archivo.name.lower()
    if nombre.endswith(".csv"):
        return pd.read_csv(archivo)
    return pd.read_excel(archivo)



def estandarizar_columnas_importacion(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    nuevas = {}
    usados = set()
    for col in df.columns:
        canon = MAPA_ENCABEZADOS.get(normalizar_encabezado(col))
        if canon:
            if canon in usados:
                raise ValueError(f"La columna '{col}' duplica el campo '{canon}'.")
            nuevas[col] = canon
            usados.add(canon)
        else:
            nuevas[col] = limpiar_texto(col)
    df = df.rename(columns=nuevas)
    return df



def preparar_dataframe_importacion(df: pd.DataFrame) -> pd.DataFrame:
    df = estandarizar_columnas_importacion(df)
    df.columns = [limpiar_texto(c) for c in df.columns]
    return df



def validar_importacion_jugadores(df: pd.DataFrame) -> dict:
    trabajo = preparar_dataframe_importacion(df)
    columnas_presentes = list(trabajo.columns)
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in columnas_presentes]
    columnas_no_admitidas = [c for c in columnas_presentes if c not in COLUMNAS_ADMITIDAS]

    if faltantes:
        return {
            "ok": False,
            "errores_generales": [f"Faltan columnas obligatorias: {', '.join(faltantes)}"],
            "columnas_presentes": columnas_presentes,
            "columnas_no_admitidas": columnas_no_admitidas,
            "df_original": trabajo,
            "df_validos": pd.DataFrame(),
            "df_invalidos": pd.DataFrame(),
        }

    if "Observaciones" not in trabajo.columns:
        trabajo["Observaciones"] = ""
    if "Activo" not in trabajo.columns:
        trabajo["Activo"] = 1

    trabajo["Nombre"] = trabajo["Nombre"].apply(limpiar_texto)
    trabajo["Apellido"] = trabajo["Apellido"].apply(limpiar_texto)
    trabajo["DNI_original"] = trabajo["DNI"]
    trabajo["DNI"] = trabajo["DNI"].apply(normalizar_dni)
    trabajo["Edad_num"] = pd.to_numeric(trabajo["Edad"], errors="coerce")
    trabajo["Activo_num"] = trabajo["Activo"].apply(valor_activo_desde_excel)
    trabajo["Observaciones"] = trabajo["Observaciones"].apply(limpiar_texto)

    duplicados_archivo = trabajo["DNI"].duplicated(keep=False) & trabajo["DNI"].ne("")
    dnis_existentes = set(obtener_jugadores(incluir_inactivos=True)["dni"].astype(str).tolist())
    existe_en_bd = trabajo["DNI"].isin(dnis_existentes)

    errores_fila = []
    for idx, row in trabajo.iterrows():
        errores = []
        if not row["Nombre"]:
            errores.append("Nombre vacío")
        if not row["Apellido"]:
            errores.append("Apellido vacío")
        if not row["DNI"]:
            errores.append("DNI vacío o inválido")
        if pd.isna(row["Edad_num"]):
            errores.append("Edad inválida")
        elif int(row["Edad_num"]) < 0 or int(row["Edad_num"]) > 99:
            errores.append("Edad fuera de rango")
        if duplicados_archivo.loc[idx]:
            errores.append("DNI duplicado dentro del archivo")
        if existe_en_bd.loc[idx]:
            errores.append("DNI ya existe en la base")
        errores_fila.append(" | ".join(errores) if errores else "")

    trabajo["errores"] = errores_fila
    trabajo["ya_existe_en_bd"] = existe_en_bd
    df_invalidos = trabajo[trabajo["errores"] != ""].copy()
    df_validos = trabajo[trabajo["errores"] == ""].copy()

    errores_generales = []
    if columnas_no_admitidas:
        errores_generales.append(
            f"Hay columnas no reconocidas que se ignorarán: {', '.join(columnas_no_admitidas)}"
        )

    return {
        "ok": len(faltantes) == 0,
        "errores_generales": errores_generales,
        "columnas_presentes": columnas_presentes,
        "columnas_no_admitidas": columnas_no_admitidas,
        "df_original": trabajo,
        "df_validos": df_validos,
        "df_invalidos": df_invalidos,
        "total_filas": len(trabajo),
        "filas_validas": len(df_validos),
        "filas_invalidas": len(df_invalidos),
        "filas_duplicadas_archivo": int(duplicados_archivo.sum()),
        "filas_existentes_bd": int(existe_en_bd.sum()),
    }



def importar_jugadores_desde_validacion(validacion: dict, modo: str):
    df = validacion["df_original"].copy()
    insertados = 0
    actualizados = 0
    omitidos = 0

    for _, row in df.iterrows():
        nombre = limpiar_texto(row["Nombre"])
        apellido = limpiar_texto(row["Apellido"])
        dni = normalizar_dni(row["DNI"])
        observaciones = limpiar_texto(row.get("Observaciones", ""))
        edad = int(row["Edad_num"]) if pd.notna(row["Edad_num"]) else None
        activo = int(row.get("Activo_num", 1))

        if not nombre or not apellido or not dni or edad is None:
            omitidos += 1
            continue

        existente = obtener_jugador_por_dni(dni)
        if existente is None:
            agregar_jugador(nombre, apellido, dni, edad, observaciones, activo=activo)
            insertados += 1
        else:
            if modo == "actualizar_existentes":
                actualizar_jugador(existente[0], nombre, apellido, dni, edad, activo, observaciones)
                actualizados += 1
            else:
                omitidos += 1

    registrar_auditoria(
        entidad="importacion_jugadores",
        accion="importacion",
        detalle=f"Importación completada. Insertados={insertados}, actualizados={actualizados}, omitidos={omitidos}",
    )
    return insertados, actualizados, omitidos



def jugadores_a_csv_bytes() -> bytes:
    df = obtener_jugadores(incluir_inactivos=True).copy()
    if df.empty:
        df = pd.DataFrame(columns=["id", "nombre", "apellido", "dni", "edad", "activo", "observaciones"])
    return df.to_csv(index=False).encode("utf-8-sig")



def pagos_a_csv_bytes() -> bytes:
    df = obtener_pagos().copy()
    if df.empty:
        df = pd.DataFrame(columns=["id", "jugador", "dni", "fecha_pago", "monto", "mes_correspondiente", "observaciones"])
    return df.to_csv(index=False).encode("utf-8-sig")



def plantilla_importacion_jugadores_xlsx() -> bytes:
    df = pd.DataFrame(
        [
            {
                "Nombre": "Juan",
                "Apellido": "Pérez",
                "DNI": "12345678",
                "Edad": 12,
                "Observaciones": "Arquero",
            },
            {
                "Nombre": "Benjamín",
                "Apellido": "Ortiz",
                "DNI": "99001122",
                "Edad": 12,
                "Observaciones": "",
            },
        ]
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Jugadores")
    output.seek(0)
    return output.getvalue()


# =========================================================
# INICIALIZACIÓN DE APP
# =========================================================
try:
    faltantes = verificar_tablas_requeridas()
    if faltantes:
        st.error(
            "La base de Supabase no tiene todas las tablas necesarias. Faltan: "
            + ", ".join(faltantes)
            + ". Ejecutá primero el SQL base en el SQL Editor."
        )
        st.stop()

    asegurar_cliente_base()
    inicializar_configuracion()
except Exception as e:
    st.error(f"No se pudo conectar o inicializar la base de datos remota: {e}")
    st.stop()

if "importacion_validada" not in st.session_state:
    st.session_state["importacion_validada"] = None


# =========================================================
# INTERFAZ
# =========================================================
organizacion_nombre = obtener_config("organizacion_nombre", "Escuelita")
moneda = obtener_config("moneda", "ARS")
mes_hoy = mes_actual_str()

st.title(f"Gestión de jugadores - {organizacion_nombre}")

with st.sidebar:
    st.subheader("Estado de la app")
    st.write(f"**Organización:** {organizacion_nombre}")
    st.write(f"**Cliente lógico actual:** `{APP_CLIENTE_ID}`")
    st.caption(
        "Versión conectada a Supabase/PostgreSQL, con importación validada y sin carga automática desde archivos del repositorio."
    )

    st.download_button(
        "Descargar plantilla Excel (.xlsx)",
        data=plantilla_importacion_jugadores_xlsx(),
        file_name="plantilla_jugadores.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.download_button(
        "Exportar jugadores (CSV)",
        data=jugadores_a_csv_bytes(),
        file_name="jugadores_export.csv",
        mime="text/csv",
        use_container_width=True,
    )
    st.download_button(
        "Exportar pagos (CSV)",
        data=pagos_a_csv_bytes(),
        file_name="pagos_export.csv",
        mime="text/csv",
        use_container_width=True,
    )

st.info(
    "Esta versión prioriza alertas del mes actual, filtra automáticamente los deudores al registrar pagos, "
    "permite editar monto y observaciones en el historial, y ofrece plantilla Excel descargable."
)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    ["Resumen", "Jugadores", "Pagos", "Importación", "Auditoría", "Configuración"]
)

# =========================================================
# TAB 1 - RESUMEN
# =========================================================
with tab1:
    st.subheader("Estado general")
    resumen = obtener_resumen_estado()

    if resumen.empty:
        st.info("No hay jugadores cargados todavía.")
    else:
        pagos_mes = obtener_pagos(mes_hoy)
        pagaron_mes_ids = set(pagos_mes["jugador_id"].tolist()) if not pagos_mes.empty else set()

        resumen = resumen.copy()
        resumen["ultimo_mes_pagado"] = resumen["ultimo_mes_pagado"].astype("object")
        resumen["estado_mes_actual"] = resumen.apply(
            lambda row: "Inactivo"
            if int(row["activo"]) == 0
            else ("Al día" if int(row["id"]) in pagaron_mes_ids else "Debe mes actual"),
            axis=1,
        )

        total = len(resumen)
        activos = int((resumen["activo"] == 1).sum())
        al_dia = int(((resumen["activo"] == 1) & (resumen["estado_mes_actual"] == "Al día")).sum())
        con_alerta = int(((resumen["activo"] == 1) & (resumen["estado_mes_actual"] == "Debe mes actual")).sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total jugadores", total)
        c2.metric("Activos", activos)
        c3.metric(f"Al día ({mes_hoy})", al_dia)
        c4.metric(f"Con alerta ({mes_hoy})", con_alerta)

        st.markdown("### Alertas del mes actual")
        alertas = resumen[(resumen["activo"] == 1) & (resumen["estado_mes_actual"] == "Debe mes actual")].copy()
        if alertas.empty:
            st.success(f"Todos los jugadores activos están al día para el mes {mes_hoy}.")
        else:
            st.warning(f"Se detectaron {len(alertas)} jugadores activos sin pago registrado para el mes {mes_hoy}.")
            st.dataframe(
                alertas[["nombre", "apellido", "dni", "edad", "observaciones", "estado_mes_actual"]],
                use_container_width=True,
                height=min(420, 70 + len(alertas) * 35),
            )

        st.markdown("### Estado por jugador")
        mostrar = resumen.copy()
        mostrar["activo"] = mostrar["activo"].map({1: "Sí", 0: "No"})
        st.dataframe(
            mostrar[
                [
                    "nombre",
                    "apellido",
                    "dni",
                    "edad",
                    "activo",
                    "ultimo_mes_pagado",
                    "ultima_fecha_pago",
                    "estado_mes_actual",
                    "observaciones",
                ]
            ],
            use_container_width=True,
            height=420,
        )

# =========================================================
# TAB 2 - JUGADORES
# =========================================================
with tab2:
    st.subheader("Gestión de jugadores")
    jugadores_df = obtener_jugadores(incluir_inactivos=True)

    filtro_texto = st.text_input("Buscar jugador por nombre, apellido o DNI")
    if filtro_texto.strip() and not jugadores_df.empty:
        patron = filtro_texto.strip().lower()
        jugadores_df = jugadores_df[
            jugadores_df.apply(
                lambda row: patron in f"{limpiar_texto(row['nombre'])} {limpiar_texto(row['apellido'])} {limpiar_texto(row['dni'])}".lower(),
                axis=1,
            )
        ]

    col_alta, col_edicion = st.columns([0.9, 1.3])

    with col_alta:
        st.markdown("### Alta manual")
        with st.form("alta_jugador"):
            nombre = st.text_input("Nombre")
            apellido = st.text_input("Apellido")
            dni = st.text_input("DNI")
            edad = st.number_input("Edad", min_value=0, max_value=99, value=7, step=1)
            observaciones = st.text_area("Observaciones", height=100)
            enviar_alta = st.form_submit_button("Guardar jugador")

            if enviar_alta:
                try:
                    if not limpiar_texto(nombre):
                        st.error("El nombre es obligatorio.")
                    elif not limpiar_texto(apellido):
                        st.error("El apellido es obligatorio.")
                    elif not normalizar_dni(dni):
                        st.error("El DNI es obligatorio.")
                    elif obtener_jugador_por_dni(dni) is not None:
                        st.error("Ya existe un jugador con ese DNI.")
                    else:
                        agregar_jugador(nombre, apellido, dni, int(edad), observaciones, activo=1)
                        st.success("Jugador agregado.")
                        st.rerun()
                except IntegrityError:
                    st.error("No se pudo guardar: conflicto de DNI.")
                except Exception as e:
                    st.error(f"Error al guardar: {e}")

    with col_edicion:
        st.markdown("### Editar / activar / desactivar")
        if jugadores_df.empty:
            st.info("No hay jugadores para gestionar con el filtro actual.")
        else:
            jugadores_df = jugadores_df.copy()
            jugadores_df["etiqueta"] = jugadores_df.apply(label_jugador, axis=1)
            elegido = st.selectbox("Seleccionar jugador", jugadores_df["etiqueta"].tolist())
            fila = jugadores_df[jugadores_df["etiqueta"] == elegido].iloc[0]

            with st.form("editar_jugador"):
                nombre_e = st.text_input("Nombre", value=fila["nombre"])
                apellido_e = st.text_input("Apellido", value=fila["apellido"])
                dni_e = st.text_input("DNI", value=fila["dni"])
                edad_e = st.number_input(
                    "Edad",
                    min_value=0,
                    max_value=99,
                    value=int(fila["edad"]) if pd.notna(fila["edad"]) else 0,
                    step=1,
                )
                activo_e = st.checkbox("Activo", value=bool(fila["activo"]))
                obs_e = st.text_area("Observaciones", value=limpiar_texto(fila["observaciones"]), height=100)
                c1, c2 = st.columns(2)
                guardar_edicion = c1.form_submit_button("Guardar cambios")
                cambiar_estado = c2.form_submit_button("Aplicar estado")

                if guardar_edicion:
                    try:
                        existente = obtener_jugador_por_dni(dni_e)
                        if existente is not None and int(existente[0]) != int(fila["id"]):
                            st.error("Ya existe otro jugador con ese DNI.")
                        elif not limpiar_texto(nombre_e) or not limpiar_texto(apellido_e):
                            st.error("Nombre y apellido son obligatorios.")
                        elif not normalizar_dni(dni_e):
                            st.error("El DNI es obligatorio.")
                        else:
                            actualizar_jugador(
                                int(fila["id"]),
                                nombre_e,
                                apellido_e,
                                dni_e,
                                int(edad_e),
                                int(activo_e),
                                obs_e,
                            )
                            st.success("Jugador actualizado.")
                            st.rerun()
                    except IntegrityError:
                        st.error("No se pudo guardar: conflicto de DNI.")
                    except Exception as e:
                        st.error(f"Error al actualizar: {e}")

                if cambiar_estado:
                    try:
                        cambiar_estado_jugador(int(fila["id"]), int(activo_e))
                        st.success("Estado del jugador actualizado.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al aplicar estado: {e}")

    st.markdown("### Base actual")
    base_df = obtener_jugadores(incluir_inactivos=True)
    if filtro_texto.strip() and not base_df.empty:
        patron = filtro_texto.strip().lower()
        base_df = base_df[
            base_df.apply(
                lambda row: patron in f"{limpiar_texto(row['nombre'])} {limpiar_texto(row['apellido'])} {limpiar_texto(row['dni'])}".lower(),
                axis=1,
            )
        ]

    if base_df.empty:
        st.info("No hay jugadores para mostrar.")
    else:
        mostrar_j = base_df.copy()
        mostrar_j["activo"] = mostrar_j["activo"].map({1: "Sí", 0: "No"})
        st.dataframe(
            mostrar_j[["nombre", "apellido", "dni", "edad", "activo", "observaciones", "fecha_alta"]],
            use_container_width=True,
            height=420,
        )

# =========================================================
# TAB 3 - PAGOS
# =========================================================
with tab3:
    st.subheader("Pagos")

    mes_registro = st.text_input("Mes de trabajo (YYYY-MM)", value=mes_hoy, key="mes_pagos")
    if not es_mes_valido(mes_registro):
        st.error("El mes de trabajo debe tener formato YYYY-MM.")
    else:
        deudores_df = obtener_jugadores_deben_mes(mes_registro).copy()
        historial_df = obtener_pagos().copy()

        col_pago, col_hist = st.columns([0.85, 1.65])

        with col_pago:
            st.markdown("### Registrar pago")
            st.caption(f"Se muestran solo jugadores activos sin pago registrado para el mes {mes_registro}.")

            if deudores_df.empty:
                st.success(f"Todos los jugadores activos están al día para el mes {mes_registro}.")
            else:
                deudores_df["etiqueta"] = deudores_df.apply(label_jugador, axis=1)
                st.write(f"**Pendientes del mes {mes_registro}:** {len(deudores_df)}")
                with st.form("registrar_pago"):
                    elegido = st.selectbox("Jugador", deudores_df["etiqueta"].tolist())
                    fila = deudores_df[deudores_df["etiqueta"] == elegido].iloc[0]
                    fecha_pago = st.date_input("Fecha de pago", value=date.today())
                    monto = st.number_input("Monto", min_value=0.0, value=0.0, step=100.0)
                    observaciones = st.text_area("Observaciones", height=90)
                    enviar_pago = st.form_submit_button("Registrar pago")

                    if enviar_pago:
                        try:
                            if monto <= 0:
                                st.error("El monto debe ser mayor a 0.")
                            else:
                                registrar_pago(
                                    jugador_id=int(fila["id"]),
                                    fecha_pago=str(fecha_pago),
                                    monto=float(monto),
                                    mes_correspondiente=mes_registro.strip(),
                                    observaciones=observaciones.strip(),
                                )
                                st.success("Pago registrado.")
                                st.rerun()
                        except IntegrityError:
                            st.error("Ya existe un pago registrado para ese jugador y ese mes.")
                        except Exception as e:
                            st.error(f"Error al registrar pago: {e}")

        with col_hist:
            st.markdown("### Historial de pagos")
            filtro_historial = st.radio(
                "Ver historial",
                options=["Mes actual", "Todos"],
                horizontal=True,
                index=0,
            )
            pagos_vista = historial_df.copy()
            if filtro_historial == "Mes actual":
                pagos_vista = pagos_vista[pagos_vista["mes_correspondiente"] == mes_registro]

            if pagos_vista.empty:
                st.info("No hay pagos para mostrar con el filtro actual.")
            else:
                editor_source = pagos_vista[["id", "jugador", "dni", "fecha_pago", "monto", "mes_correspondiente", "observaciones"]].copy()
                editor_source["observaciones"] = editor_source["observaciones"].fillna("")
                editor_source = editor_source.set_index("id")
                editor_source.index.name = "id"

                edited_df = st.data_editor(
                    editor_source,
                    use_container_width=True,
                    height=430,
                    key="editor_pagos_historial",
                    disabled=["jugador", "dni", "fecha_pago", "mes_correspondiente"],
                    column_config={
                        "jugador": st.column_config.TextColumn("Jugador", width="medium"),
                        "dni": st.column_config.TextColumn("DNI", width="small"),
                        "fecha_pago": st.column_config.TextColumn("Fecha", width="small"),
                        "monto": st.column_config.NumberColumn("Monto", min_value=0.0, step=100.0, format="%.2f"),
                        "mes_correspondiente": st.column_config.TextColumn("Mes", width="small"),
                        "observaciones": st.column_config.TextColumn("Observaciones", width="large"),
                    },
                )

                cambios = []
                for idx in edited_df.index:
                    monto_original = float(editor_source.loc[idx, "monto"])
                    monto_nuevo = float(edited_df.loc[idx, "monto"])
                    obs_original = limpiar_texto(editor_source.loc[idx, "observaciones"])
                    obs_nueva = limpiar_texto(edited_df.loc[idx, "observaciones"])
                    if monto_original != monto_nuevo or obs_original != obs_nueva:
                        cambios.append((idx, monto_nuevo, obs_nueva))

                c_guardar, c_info = st.columns([1, 2])
                with c_guardar:
                    if st.button("Guardar cambios del historial", use_container_width=True):
                        try:
                            if not cambios:
                                st.info("No hay cambios para guardar.")
                            else:
                                for pago_id, monto_nuevo, obs_nueva in cambios:
                                    actualizar_pago(int(pago_id), monto_nuevo, obs_nueva)
                                st.success(f"Se guardaron {len(cambios)} cambios en el historial.")
                                st.rerun()
                        except Exception as e:
                            st.error(f"No se pudieron guardar los cambios: {e}")
                with c_info:
                    st.caption(
                        "Podés editar directamente el monto y las observaciones del historial. "
                        "Los cambios se guardan con el botón de abajo."
                    )

# =========================================================
# TAB 4 - IMPORTACIÓN
# =========================================================
with tab4:
    st.subheader("Carga inicial e importación de jugadores")
    st.caption("La carga inicial es manual y validada antes de guardar.")

    ctpl1, ctpl2 = st.columns([1, 2])
    with ctpl1:
        st.download_button(
            "Descargar plantilla Excel (.xlsx)",
            data=plantilla_importacion_jugadores_xlsx(),
            file_name="plantilla_jugadores.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with ctpl2:
        st.write(
            "La plantilla incluye las columnas recomendadas: Nombre, Apellido, DNI, Edad y Observaciones. "
            "El campo Activo no es obligatorio: al importar se carga activo por defecto."
        )

    archivo = st.file_uploader("Subir archivo de jugadores", type=["xlsx", "csv"], key="uploader_jugadores")
    c_validar, c_limpiar = st.columns([1, 1])
    with c_validar:
        validar = st.button("Validar archivo", use_container_width=True)
    with c_limpiar:
        limpiar_importacion = st.button("Limpiar validación", use_container_width=True)

    if limpiar_importacion:
        st.session_state["importacion_validada"] = None
        st.rerun()

    if validar:
        if archivo is None:
            st.warning("Primero subí un archivo.")
        else:
            try:
                df_subido = leer_archivo_carga(archivo)
                validacion = validar_importacion_jugadores(df_subido)
                st.session_state["importacion_validada"] = validacion
                st.success("Archivo procesado. Revisá el resultado abajo.")
            except Exception as e:
                st.error(f"No se pudo leer o validar el archivo: {e}")

    validacion = st.session_state.get("importacion_validada")
    if validacion is not None:
        st.markdown("### Resultado de validación")
        for mensaje in validacion.get("errores_generales", []):
            st.warning(mensaje)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Filas totales", validacion.get("total_filas", 0))
        col2.metric("Filas válidas", validacion.get("filas_validas", 0))
        col3.metric("Filas con error", validacion.get("filas_invalidas", 0))
        col4.metric("DNI ya existentes", validacion.get("filas_existentes_bd", 0))

        st.markdown("#### Vista previa")
        st.dataframe(validacion["df_original"].head(20), use_container_width=True, height=320)

        if not validacion["df_invalidos"].empty:
            st.markdown("#### Filas con error")
            st.dataframe(
                validacion["df_invalidos"][["Nombre", "Apellido", "DNI_original", "DNI", "Edad", "errores"]],
                use_container_width=True,
                height=320,
            )
        else:
            st.success("No se encontraron errores de validación.")

        st.markdown("#### Confirmar importación")
        modo_importacion = st.radio(
            "Modo de importación",
            options=["solo_nuevos", "actualizar_existentes"],
            format_func=lambda x: "Solo agregar nuevos" if x == "solo_nuevos" else "Agregar nuevos y actualizar existentes por DNI",
            horizontal=False,
        )

        if st.button("Confirmar importación", type="primary"):
            try:
                insertados, actualizados, omitidos = importar_jugadores_desde_validacion(
                    validacion,
                    modo=modo_importacion,
                )
                st.success(
                    f"Importación completada. Insertados: {insertados}. Actualizados: {actualizados}. Omitidos: {omitidos}."
                )
                st.session_state["importacion_validada"] = None
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo completar la importación: {e}")

# =========================================================
# TAB 5 - AUDITORÍA
# =========================================================
with tab5:
    st.subheader("Auditoría")
    limite = st.selectbox("Cantidad de eventos", options=[20, 50, 100, 200], index=2)
    aud_df = obtener_auditoria(limite=limite)
    if aud_df.empty:
        st.info("No hay eventos registrados.")
    else:
        st.dataframe(aud_df, use_container_width=True, height=430)

# =========================================================
# TAB 6 - CONFIGURACIÓN
# =========================================================
with tab6:
    st.subheader("Configuración general")

    st.markdown("### Mantenimiento de datos")
    st.warning(
        "Esta acción elimina todos los jugadores y pagos de la organización actual. "
        "No borra la estructura base ni vuelve a cargar datos automáticamente desde archivos del repositorio."
    )
    with st.form("vaciar_base_operativa"):
        confirmacion_borrado = st.text_input("Escribí BORRAR para confirmar el vaciado total de jugadores y pagos")
        ejecutar_borrado = st.form_submit_button("Vaciar base operativa")

        if ejecutar_borrado:
            try:
                if confirmacion_borrado.strip() != "BORRAR":
                    st.error('Confirmación inválida. Escribí exactamente BORRAR para continuar.')
                else:
                    total_jugadores, total_pagos = borrar_base_operativa(conservar_auditoria=True)
                    st.success(
                        f"Base operativa vaciada. Se eliminaron {total_jugadores} jugadores y {total_pagos} pagos."
                    )
                    st.rerun()
            except Exception as e:
                st.error(f"No se pudo vaciar la base operativa: {e}")

    st.markdown("### Parámetros generales")
    with st.form("configuracion_app"):
        nombre_org = st.text_input("Nombre de la organización", value=organizacion_nombre)
        moneda_in = st.text_input("Moneda", value=moneda)
        guardar = st.form_submit_button("Guardar configuración")

        if guardar:
            try:
                guardar_config("organizacion_nombre", limpiar_texto(nombre_org) or "Escuelita")
                guardar_config("moneda", limpiar_texto(moneda_in) or "ARS")
                registrar_auditoria(
                    entidad="configuracion_app",
                    accion="edicion",
                    detalle="Actualización de configuración general",
                )
                st.success("Configuración actualizada.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar la configuración: {e}")
