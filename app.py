import os
import re
from datetime import date, datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

st.set_page_config(page_title="Gestión de jugadores", layout="wide")

# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================
APP_CLIENTE_ID = "default"
COLUMNAS_IMPORTACION_REQUERIDAS = ["Nombre", "Apellido", "DNI", "Edad"]
COLUMNAS_IMPORTACION_ADMITIDAS = ["Nombre", "Apellido", "DNI", "Edad", "Observaciones", "Activo"]


# =========================================================
# UTILIDADES
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
    texto = str(valor).strip().lower()
    if texto in {"0", "no", "false", "falso", "inactivo"}:
        return 0
    return 1


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
    database_url = obtener_database_url()
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True,
    )


def ejecutar_select(query: str, params=None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn, params=params or {})


def ejecutar_accion(query: str, params=None):
    engine = get_engine()
    with engine.begin() as conn:
        return conn.execute(text(query), params or {})


# =========================================================
# VALIDACIONES DE ESQUEMA
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
    faltantes = sorted(list(requeridas - existentes))
    return faltantes


def inicializar_configuracion():
    ejecutar_accion(
        """
        insert into public.configuracion_app (clave, valor)
        values (:clave, :valor)
        on conflict (clave) do update set valor = excluded.valor
        """,
        {"clave": "organizacion_nombre", "valor": "Escuelita"},
    )
    ejecutar_accion(
        """
        insert into public.configuracion_app (clave, valor)
        values (:clave, :valor)
        on conflict (clave) do update set valor = excluded.valor
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
# CONSULTAS Y OPERACIONES
# =========================================================
def obtener_jugadores(incluir_inactivos=True) -> pd.DataFrame:
    query = """
    select id, cliente_id, nombre, apellido, dni, edad, activo, observaciones, fecha_alta, fecha_actualizacion
    from public.jugadores
    where cliente_id = :cliente_id
    """
    if not incluir_inactivos:
        query += " and activo = 1"
    query += " order by apellido, nombre"
    return ejecutar_select(query, {"cliente_id": APP_CLIENTE_ID})



def obtener_pagos() -> pd.DataFrame:
    query = """
    select p.id,
           p.jugador_id,
           j.nombre || case when trim(j.apellido) <> '' then ' ' || j.apellido else '' end as jugador,
           j.dni,
           p.fecha_pago,
           p.monto,
           p.mes_correspondiente,
           p.observaciones,
           p.fecha_creacion
    from public.pagos p
    inner join public.jugadores j on p.jugador_id = j.id and p.cliente_id = j.cliente_id
    where p.cliente_id = :cliente_id
    order by p.fecha_pago desc, p.id desc
    """
    return ejecutar_select(query, {"cliente_id": APP_CLIENTE_ID})



def obtener_auditoria(limite=100) -> pd.DataFrame:
    query = """
    select id, entidad, entidad_id, accion, detalle, fecha_evento
    from public.auditoria
    where cliente_id = :cliente_id
    order by id desc
    limit :limite
    """
    return ejecutar_select(query, {"cliente_id": APP_CLIENTE_ID, "limite": limite})



def obtener_resumen_estado() -> pd.DataFrame:
    query = """
    select j.id,
           j.nombre,
           j.apellido,
           j.dni,
           j.edad,
           j.activo,
           max(p.mes_correspondiente) as ultimo_mes_pagado,
           max(p.fecha_pago) as ultima_fecha_pago
    from public.jugadores j
    left join public.pagos p
        on p.jugador_id = j.id
       and p.cliente_id = j.cliente_id
    where j.cliente_id = :cliente_id
    group by j.id, j.nombre, j.apellido, j.dni, j.edad, j.activo
    order by j.apellido, j.nombre
    """
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



def jugadores_vacios() -> bool:
    df = ejecutar_select(
        "select count(*) as total from public.jugadores where cliente_id = :cliente_id",
        {"cliente_id": APP_CLIENTE_ID},
    )
    return int(df.iloc[0]["total"]) == 0



def clasificar_estado_pago(ultimo_mes_pagado):
    if ultimo_mes_pagado is None or pd.isna(ultimo_mes_pagado):
        return "Sin pagos"
    actual = mes_actual_str()
    if str(ultimo_mes_pagado) == actual:
        return "Al día"
    try:
        y_u, m_u = map(int, str(ultimo_mes_pagado).split("-"))
        y_a, m_a = map(int, actual.split("-"))
        diff = (y_a - y_u) * 12 + (m_a - m_u)
        if diff == 1:
            return "Debe 1 mes"
        if diff >= 2:
            return f"Debe {diff} meses"
        return "Revisar"
    except Exception:
        return "Revisar"



def agregar_jugador(nombre, apellido, dni, edad, observaciones, activo=1):
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



def baja_logica_jugador(jugador_id):
    ejecutar_accion(
        """
        update public.jugadores
        set activo = 0, fecha_actualizacion = :fecha_actualizacion
        where id = :jugador_id and cliente_id = :cliente_id
        """,
        {"fecha_actualizacion": ahora_str(), "jugador_id": jugador_id, "cliente_id": APP_CLIENTE_ID},
    )
    registrar_auditoria(
        entidad="jugadores",
        entidad_id=jugador_id,
        accion="baja_logica",
        detalle="Jugador marcado como inactivo",
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


# =========================================================
# IMPORTACIÓN DE DATOS
# =========================================================
def leer_archivo_carga(archivo) -> pd.DataFrame:
    nombre = archivo.name.lower()
    if nombre.endswith(".csv"):
        return pd.read_csv(archivo)
    return pd.read_excel(archivo)



def preparar_dataframe_importacion(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df



def validar_importacion_jugadores(df: pd.DataFrame) -> dict:
    df = preparar_dataframe_importacion(df)

    columnas_presentes = list(df.columns)
    faltantes = [c for c in COLUMNAS_IMPORTACION_REQUERIDAS if c not in columnas_presentes]
    columnas_no_admitidas = [c for c in columnas_presentes if c not in COLUMNAS_IMPORTACION_ADMITIDAS]

    if faltantes:
        return {
            "ok": False,
            "errores_generales": [f"Faltan columnas obligatorias: {', '.join(faltantes)}"],
            "columnas_presentes": columnas_presentes,
            "columnas_no_admitidas": columnas_no_admitidas,
            "df_original": df,
            "df_validos": pd.DataFrame(),
            "df_invalidos": pd.DataFrame(),
        }

    trabajo = df.copy()
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

    errores_fila = []

    duplicados_archivo = trabajo["DNI"].duplicated(keep=False) & trabajo["DNI"].ne("")

    dnis_existentes = set(obtener_jugadores(incluir_inactivos=True)["dni"].astype(str).tolist())
    existe_en_bd = trabajo["DNI"].isin(dnis_existentes)

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





def borrar_base_operativa(conservar_auditoria: bool = True):
    jugadores_df = obtener_jugadores(incluir_inactivos=True)
    pagos_df = obtener_pagos()

    total_jugadores = len(jugadores_df)
    total_pagos = len(pagos_df)

    ejecutar_accion(
        """
        delete from public.pagos
        where cliente_id = :cliente_id
        """,
        {"cliente_id": APP_CLIENTE_ID},
    )

    ejecutar_accion(
        """
        delete from public.jugadores
        where cliente_id = :cliente_id
        """,
        {"cliente_id": APP_CLIENTE_ID},
    )

    if not conservar_auditoria:
        ejecutar_accion(
            """
            delete from public.auditoria
            where cliente_id = :cliente_id
            """,
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
# EXPORTACIÓN
# =========================================================
def jugadores_a_csv_bytes() -> bytes:
    df = obtener_jugadores(incluir_inactivos=True).copy()
    if df.empty:
        df = pd.DataFrame(columns=["id", "nombre", "apellido", "dni", "edad", "activo", "observaciones"])
    return df.to_csv(index=False).encode("utf-8-sig")



def pagos_a_csv_bytes() -> bytes:
    df = obtener_pagos().copy()
    if df.empty:
        df = pd.DataFrame(columns=["id", "jugador", "dni", "fecha_pago", "monto", "mes_correspondiente"])
    return df.to_csv(index=False).encode("utf-8-sig")



def plantilla_importacion_jugadores() -> bytes:
    df = pd.DataFrame(
        [
            {
                "Nombre": "Juan",
                "Apellido": "Pérez",
                "DNI": "12345678",
                "Edad": 12,
                "Observaciones": "Arquero",
                "Activo": "SI",
            }
        ]
    )
    return df.to_csv(index=False).encode("utf-8-sig")


# =========================================================
# INICIALIZACIÓN
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

st.title(f"Gestión de jugadores - {organizacion_nombre}")

with st.sidebar:
    st.subheader("Estado de la app")
    st.write(f"**Organización:** {organizacion_nombre}")
    st.write(f"**Cliente lógico actual:** `{APP_CLIENTE_ID}`")
    st.caption(
        "Esta versión ya usa PostgreSQL remoto en Supabase. "
        "La app espera una variable DATABASE_URL con la URI del Session Pooler."
    )

    st.download_button(
        "Descargar plantilla de importación",
        data=plantilla_importacion_jugadores(),
        file_name="plantilla_jugadores.csv",
        mime="text/csv",
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
    "Cambios clave de esta versión: conexión a Supabase/PostgreSQL, importación validada antes de guardar, "
    "auditoría básica, modelo preparado para multi-cliente y sin importación automática desde archivos del repositorio."
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
        resumen["estado_pago"] = resumen["ultimo_mes_pagado"].apply(clasificar_estado_pago)
        total = len(resumen)
        activos = int((resumen["activo"] == 1).sum())
        al_dia = int((resumen["estado_pago"] == "Al día").sum())
        con_deuda = int(resumen["estado_pago"].astype(str).str.contains("Debe|Sin pagos", na=False).sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total jugadores", total)
        c2.metric("Activos", activos)
        c3.metric("Al día", al_dia)
        c4.metric("Con alertas", con_deuda)

        st.markdown("### Estado por jugador")
        mostrar = resumen.copy()
        mostrar["activo"] = mostrar["activo"].map({1: "Sí", 0: "No"})
        st.dataframe(
            mostrar[
                ["nombre", "apellido", "dni", "edad", "activo", "ultimo_mes_pagado", "ultima_fecha_pago", "estado_pago"]
            ],
            use_container_width=True,
        )

        st.markdown("### Alertas")
        alertas = mostrar[mostrar["estado_pago"].astype(str).str.contains("Debe|Sin pagos", na=False)]
        if alertas.empty:
            st.success("No hay jugadores con deuda o sin pagos.")
        else:
            st.warning("Se detectaron jugadores con deuda o sin pagos registrados.")
            st.dataframe(
                alertas[["nombre", "apellido", "dni", "ultimo_mes_pagado", "estado_pago"]],
                use_container_width=True,
            )

# =========================================================
# TAB 2 - JUGADORES
# =========================================================
with tab2:
    st.subheader("Gestión de jugadores")

    jugadores_df = obtener_jugadores(incluir_inactivos=True)
    col_alta, col_edicion = st.columns([1, 1.4])

    with col_alta:
        st.markdown("### Alta manual")
        with st.form("alta_jugador"):
            nombre = st.text_input("Nombre")
            apellido = st.text_input("Apellido")
            dni = st.text_input("DNI")
            edad = st.number_input("Edad", min_value=0, max_value=99, value=7, step=1)
            observaciones = st.text_area("Observaciones")
            activo = st.checkbox("Activo", value=True)
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
                        agregar_jugador(nombre, apellido, dni, int(edad), observaciones, activo=int(activo))
                        st.success("Jugador agregado.")
                        st.rerun()
                except IntegrityError:
                    st.error("No se pudo guardar: conflicto de DNI.")
                except Exception as e:
                    st.error(f"Error al guardar: {e}")

    with col_edicion:
        st.markdown("### Editar o dar de baja")
        if jugadores_df.empty:
            st.info("No hay jugadores.")
        else:
            jugadores_df["etiqueta"] = (
                jugadores_df["apellido"].fillna("") + ", " +
                jugadores_df["nombre"].fillna("") + " - DNI " +
                jugadores_df["dni"].fillna("")
            )
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
                obs_e = st.text_area("Observaciones", value=fila["observaciones"] if pd.notna(fila["observaciones"]) else "")
                c1, c2 = st.columns(2)
                guardar_edicion = c1.form_submit_button("Guardar cambios")
                dar_baja = c2.form_submit_button("Dar de baja lógica")

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
                                activo_e,
                                obs_e,
                            )
                            st.success("Jugador actualizado.")
                            st.rerun()
                    except IntegrityError:
                        st.error("No se pudo guardar: conflicto de DNI.")
                    except Exception as e:
                        st.error(f"Error al actualizar: {e}")

                if dar_baja:
                    try:
                        baja_logica_jugador(int(fila["id"]))
                        st.success("Jugador marcado como inactivo.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al dar de baja: {e}")

    st.markdown("### Base actual")
    if jugadores_df.empty:
        st.info("No hay jugadores para mostrar.")
    else:
        mostrar_j = jugadores_df.copy()
        mostrar_j["activo"] = mostrar_j["activo"].map({1: "Sí", 0: "No"})
        st.dataframe(
            mostrar_j[["nombre", "apellido", "dni", "edad", "activo", "observaciones", "fecha_alta", "fecha_actualizacion"]],
            use_container_width=True,
        )

# =========================================================
# TAB 3 - PAGOS
# =========================================================
with tab3:
    st.subheader("Pagos")
    jugadores_activos = obtener_jugadores(incluir_inactivos=False)

    col_pago, col_hist = st.columns([1, 1.2])

    with col_pago:
        st.markdown("### Registrar pago")
        if jugadores_activos.empty:
            st.info("No hay jugadores activos.")
        else:
            jugadores_activos["etiqueta"] = (
                jugadores_activos["apellido"].fillna("") + ", " +
                jugadores_activos["nombre"].fillna("") + " - DNI " +
                jugadores_activos["dni"].fillna("")
            )

            with st.form("registrar_pago"):
                elegido = st.selectbox("Jugador", jugadores_activos["etiqueta"].tolist())
                fila = jugadores_activos[jugadores_activos["etiqueta"] == elegido].iloc[0]

                fecha_pago = st.date_input("Fecha de pago", value=date.today())
                monto = st.number_input("Monto", min_value=0.0, value=0.0, step=100.0)
                mes_correspondiente = st.text_input("Mes correspondiente (YYYY-MM)", value=mes_actual_str())
                observaciones = st.text_area("Observaciones")
                enviar_pago = st.form_submit_button("Registrar pago")

                if enviar_pago:
                    try:
                        if monto <= 0:
                            st.error("El monto debe ser mayor a 0.")
                        elif not es_mes_valido(mes_correspondiente):
                            st.error("El mes correspondiente debe tener formato YYYY-MM.")
                        else:
                            registrar_pago(
                                jugador_id=int(fila["id"]),
                                fecha_pago=str(fecha_pago),
                                monto=float(monto),
                                mes_correspondiente=mes_correspondiente.strip(),
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
        pagos_df = obtener_pagos()
        if pagos_df.empty:
            st.info("No hay pagos registrados.")
        else:
            st.dataframe(
                pagos_df[["jugador", "dni", "fecha_pago", "monto", "mes_correspondiente", "observaciones"]],
                use_container_width=True,
            )

# =========================================================
# TAB 4 - IMPORTACIÓN
# =========================================================
with tab4:
    st.subheader("Carga inicial e importación de jugadores")
    st.caption("La app ya no importa datos automáticamente al iniciar. Toda carga se hace manualmente desde esta sección.")
    st.write(
        "Esta sección permite subir un archivo CSV o Excel con la base de jugadores. "
        "La app primero valida el contenido y recién después importa."
    )

    st.markdown("**Formato esperado**")
    st.write("Columnas obligatorias: Nombre, Apellido, DNI, Edad.")
    st.write("Columnas opcionales: Observaciones, Activo.")

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

        st.markdown("#### Vista previa del archivo")
        st.dataframe(validacion["df_original"].head(20), use_container_width=True)

        if not validacion["df_invalidos"].empty:
            st.markdown("#### Filas con error")
            st.dataframe(
                validacion["df_invalidos"][["Nombre", "Apellido", "DNI_original", "DNI", "Edad", "errores"]],
                use_container_width=True,
            )
        else:
            st.success("No se encontraron errores de validación.")

        st.markdown("#### Importación")
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
                    f"Importación completada. Insertados: {insertados}. "
                    f"Actualizados: {actualizados}. Omitidos: {omitidos}."
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
        st.dataframe(aud_df, use_container_width=True)

# =========================================================
# TAB 6 - CONFIGURACIÓN
# =========================================================
with tab6:
    st.subheader("Configuración general")

    st.markdown("### Mantenimiento de datos")
    st.warning(
        "Esta acción elimina todos los jugadores y todos los pagos de la organización actual en Supabase. "
        "No se vuelven a cargar automáticamente desde ningún archivo del repositorio."
    )
    with st.form("vaciar_base_operativa"):
        confirmacion_borrado = st.text_input(
            "Escribí BORRAR para confirmar el vaciado total de jugadores y pagos"
        )
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
