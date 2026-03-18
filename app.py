import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, date

st.set_page_config(page_title="Gestión de jugadores", layout="wide")

# =========================
# CONFIG
# =========================
LOCAL_EXCEL_PATH = Path(r"C:\Users\EEALR\OneDrive\Documentos\APLICACION\JUAGDORES_ESCUELA.xlsx")
REPO_EXCEL_PATH = Path("data/JUAGDORES_ESCUELA.xlsx")
DB_PATH = Path("data/gestion_jugadores.db")


# =========================
# UTILIDADES
# =========================
def normalizar_dni(valor):
    if pd.isna(valor):
        return ""
    return "".join(ch for ch in str(valor) if ch.isdigit())


def asegurar_carpetas():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPO_EXCEL_PATH.parent.mkdir(parents=True, exist_ok=True)


def conectar_db():
    asegurar_carpetas()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def crear_tablas(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS jugadores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        apellido TEXT,
        dni TEXT NOT NULL UNIQUE,
        edad INTEGER,
        activo INTEGER NOT NULL DEFAULT 1,
        observaciones TEXT,
        fecha_alta TEXT DEFAULT CURRENT_DATE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pagos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        jugador_id INTEGER NOT NULL,
        fecha_pago TEXT NOT NULL,
        monto REAL NOT NULL,
        mes_correspondiente TEXT NOT NULL,
        observaciones TEXT,
        FOREIGN KEY (jugador_id) REFERENCES jugadores(id) ON DELETE CASCADE
    )
    """)

    conn.commit()


def excel_origen_disponible():
    if LOCAL_EXCEL_PATH.exists():
        return LOCAL_EXCEL_PATH
    if REPO_EXCEL_PATH.exists():
        return REPO_EXCEL_PATH
    return None


def importar_excel_inicial(conn, ruta_excel):
    df = pd.read_excel(ruta_excel)

    columnas_esperadas = ["Nombre", "Apellido", "DNI", "Edad"]
    faltantes = [c for c in columnas_esperadas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas en el Excel: {faltantes}")

    df = df[columnas_esperadas].copy()
    df["DNI"] = df["DNI"].apply(normalizar_dni)
    df["Edad"] = pd.to_numeric(df["Edad"], errors="coerce")

    cur = conn.cursor()

    for _, row in df.iterrows():
        nombre = str(row["Nombre"]).strip() if pd.notna(row["Nombre"]) else ""
        apellido = str(row["Apellido"]).strip() if pd.notna(row["Apellido"]) else ""
        dni = normalizar_dni(row["DNI"])
        edad = int(row["Edad"]) if pd.notna(row["Edad"]) else None

        if not nombre or not dni:
            continue

        cur.execute("""
            INSERT OR IGNORE INTO jugadores (nombre, apellido, dni, edad, activo)
            VALUES (?, ?, ?, ?, 1)
        """, (nombre, apellido, dni, edad))

    conn.commit()


def jugadores_vacios(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM jugadores")
    return cur.fetchone()[0] == 0


def exportar_jugadores_a_excel(conn):
    query = """
    SELECT nombre AS Nombre,
           apellido AS Apellido,
           dni AS DNI,
           edad AS Edad,
           CASE WHEN activo = 1 THEN 'SI' ELSE 'NO' END AS Activo,
           observaciones AS Observaciones,
           fecha_alta AS Fecha_Alta
    FROM jugadores
    ORDER BY apellido, nombre
    """
    df = pd.read_sql_query(query, conn)
    asegurar_carpetas()
    df.to_excel(REPO_EXCEL_PATH, index=False)


def obtener_jugadores(conn, incluir_inactivos=True):
    query = """
    SELECT id, nombre, apellido, dni, edad, activo, observaciones, fecha_alta
    FROM jugadores
    """
    if not incluir_inactivos:
        query += " WHERE activo = 1"
    query += " ORDER BY apellido, nombre"
    return pd.read_sql_query(query, conn)


def obtener_pagos(conn):
    query = """
    SELECT p.id,
           j.nombre || ' ' || j.apellido AS jugador,
           j.dni,
           p.fecha_pago,
           p.monto,
           p.mes_correspondiente,
           p.observaciones
    FROM pagos p
    INNER JOIN jugadores j ON p.jugador_id = j.id
    ORDER BY p.fecha_pago DESC, p.id DESC
    """
    return pd.read_sql_query(query, conn)


def obtener_resumen_estado(conn):
    query = """
    SELECT j.id,
           j.nombre,
           j.apellido,
           j.dni,
           j.edad,
           j.activo,
           MAX(p.mes_correspondiente) AS ultimo_mes_pagado,
           MAX(p.fecha_pago) AS ultima_fecha_pago
    FROM jugadores j
    LEFT JOIN pagos p ON p.jugador_id = j.id
    GROUP BY j.id, j.nombre, j.apellido, j.dni, j.edad, j.activo
    ORDER BY j.apellido, j.nombre
    """
    df = pd.read_sql_query(query, conn)
    return df


def mes_actual_str():
    hoy = date.today()
    return f"{hoy.year}-{hoy.month:02d}"


def clasificar_estado_pago(ultimo_mes_pagado):
    if ultimo_mes_pagado is None or pd.isna(ultimo_mes_pagado):
        return "Sin pagos"
    actual = mes_actual_str()
    if ultimo_mes_pagado == actual:
        return "Al día"
    try:
        y_u, m_u = map(int, ultimo_mes_pagado.split("-"))
        y_a, m_a = map(int, actual.split("-"))
        diff = (y_a - y_u) * 12 + (m_a - m_u)
        if diff == 1:
            return "Debe 1 mes"
        if diff >= 2:
            return f"Debe {diff} meses"
        return "Revisar"
    except Exception:
        return "Revisar"


def registrar_pago(conn, jugador_id, fecha_pago, monto, mes_correspondiente, observaciones):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pagos (jugador_id, fecha_pago, monto, mes_correspondiente, observaciones)
        VALUES (?, ?, ?, ?, ?)
    """, (jugador_id, fecha_pago, monto, mes_correspondiente, observaciones))
    conn.commit()


def agregar_jugador(conn, nombre, apellido, dni, edad, observaciones):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO jugadores (nombre, apellido, dni, edad, observaciones, activo)
        VALUES (?, ?, ?, ?, ?, 1)
    """, (nombre.strip(), apellido.strip(), normalizar_dni(dni), edad, observaciones.strip()))
    conn.commit()


def actualizar_jugador(conn, jugador_id, nombre, apellido, dni, edad, activo, observaciones):
    cur = conn.cursor()
    cur.execute("""
        UPDATE jugadores
        SET nombre = ?, apellido = ?, dni = ?, edad = ?, activo = ?, observaciones = ?
        WHERE id = ?
    """, (nombre.strip(), apellido.strip(), normalizar_dni(dni), edad, int(activo), observaciones.strip(), jugador_id))
    conn.commit()


def baja_logica_jugador(conn, jugador_id):
    cur = conn.cursor()
    cur.execute("UPDATE jugadores SET activo = 0 WHERE id = ?", (jugador_id,))
    conn.commit()


# =========================
# INICIALIZACIÓN
# =========================
conn = conectar_db()
crear_tablas(conn)

if jugadores_vacios(conn):
    ruta_origen = excel_origen_disponible()
    if ruta_origen is not None:
        try:
            importar_excel_inicial(conn, ruta_origen)
            exportar_jugadores_a_excel(conn)
            st.success(f"Base inicial importada desde: {ruta_origen}")
        except Exception as e:
            st.error(f"No se pudo importar el Excel inicial: {e}")
    else:
        st.warning(
            "No se encontró el archivo inicial. "
            "En local se busca en la ruta de Windows configurada. "
            "En Streamlit Cloud tenés que subir el archivo al repo en data/JUAGDORES_ESCUELA.xlsx."
        )


# =========================
# UI
# =========================
st.title("Gestión de jugadores - Escuelita")

st.info(
    "Importante: en Streamlit Community Cloud los cambios hechos sobre archivos locales o bases locales "
    "no están garantizados como persistentes entre sesiones. Para una persistencia confiable en la nube, "
    "conviene usar una base externa. En local, este esquema sí sirve bien."
)

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Resumen", "Gestión de jugadores", "Registrar pago", "Historial de pagos", "Carga inicial"]
)

# =========================
# TAB 1 - RESUMEN
# =========================
with tab1:
    st.subheader("Estado general")
    resumen = obtener_resumen_estado(conn)

    if not resumen.empty:
        resumen["estado_pago"] = resumen["ultimo_mes_pagado"].apply(clasificar_estado_pago)

        total = len(resumen)
        activos = int((resumen["activo"] == 1).sum())
        al_dia = int((resumen["estado_pago"] == "Al día").sum())
        en_deuda = int(resumen["estado_pago"].astype(str).str.contains("Debe").sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total jugadores", total)
        c2.metric("Activos", activos)
        c3.metric("Al día", al_dia)
        c4.metric("Con deuda", en_deuda)

        st.write("Detalle:")
        mostrar = resumen.copy()
        mostrar["estado_pago"] = mostrar["estado_pago"]
        st.dataframe(mostrar, use_container_width=True)

        alertas = mostrar[mostrar["estado_pago"].astype(str).str.contains("Debe|Sin pagos", na=False)]
        st.subheader("Alertas")
        if alertas.empty:
            st.success("No hay alertas de mora.")
        else:
            st.warning("Jugadores con deuda o sin pagos registrados")
            st.dataframe(alertas[["nombre", "apellido", "dni", "ultimo_mes_pagado", "estado_pago"]], use_container_width=True)
    else:
        st.info("No hay jugadores cargados.")


# =========================
# TAB 2 - GESTIÓN DE JUGADORES
# =========================
with tab2:
    st.subheader("Alta, edición y baja lógica")

    jugadores_df = obtener_jugadores(conn, incluir_inactivos=True)

    col_a, col_b = st.columns([1, 2])

    with col_a:
        st.markdown("### Alta de jugador")
        with st.form("alta_jugador"):
            nombre = st.text_input("Nombre")
            apellido = st.text_input("Apellido")
            dni = st.text_input("DNI")
            edad = st.number_input("Edad", min_value=0, max_value=99, value=7, step=1)
            observaciones = st.text_area("Observaciones")
            enviar_alta = st.form_submit_button("Guardar jugador")

            if enviar_alta:
                try:
                    if not nombre.strip() or not dni.strip():
                        st.error("Nombre y DNI son obligatorios.")
                    else:
                        agregar_jugador(conn, nombre, apellido, dni, int(edad), observaciones)
                        exportar_jugadores_a_excel(conn)
                        st.success("Jugador agregado.")
                        st.rerun()
                except sqlite3.IntegrityError:
                    st.error("Ese DNI ya existe.")
                except Exception as e:
                    st.error(f"Error al guardar: {e}")

    with col_b:
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
                edad_e = st.number_input("Edad", min_value=0, max_value=99, value=int(fila["edad"]) if pd.notna(fila["edad"]) else 0, step=1)
                activo_e = st.checkbox("Activo", value=bool(fila["activo"]))
                obs_e = st.text_area("Observaciones", value=fila["observaciones"] if pd.notna(fila["observaciones"]) else "")
                c1, c2 = st.columns(2)
                guardar_edicion = c1.form_submit_button("Guardar cambios")
                dar_baja = c2.form_submit_button("Dar de baja")

                if guardar_edicion:
                    try:
                        actualizar_jugador(conn, int(fila["id"]), nombre_e, apellido_e, dni_e, int(edad_e), activo_e, obs_e)
                        exportar_jugadores_a_excel(conn)
                        st.success("Jugador actualizado.")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error("No se puede guardar: el DNI ya existe.")
                    except Exception as e:
                        st.error(f"Error al actualizar: {e}")

                if dar_baja:
                    try:
                        baja_logica_jugador(conn, int(fila["id"]))
                        exportar_jugadores_a_excel(conn)
                        st.success("Jugador dado de baja (baja lógica).")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al dar de baja: {e}")


# =========================
# TAB 3 - REGISTRAR PAGO
# =========================
with tab3:
    st.subheader("Registrar pago")
    jugadores_activos = obtener_jugadores(conn, incluir_inactivos=False)

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
                    else:
                        registrar_pago(
                            conn,
                            jugador_id=int(fila["id"]),
                            fecha_pago=str(fecha_pago),
                            monto=float(monto),
                            mes_correspondiente=mes_correspondiente.strip(),
                            observaciones=observaciones.strip()
                        )
                        st.success("Pago registrado.")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error al registrar pago: {e}")


# =========================
# TAB 4 - HISTORIAL DE PAGOS
# =========================
with tab4:
    st.subheader("Historial de pagos")
    pagos_df = obtener_pagos(conn)
    if pagos_df.empty:
        st.info("No hay pagos registrados.")
    else:
        st.dataframe(pagos_df, use_container_width=True)


# =========================
# TAB 5 - CARGA INICIAL
# =========================
with tab5:
    st.subheader("Carga inicial / reemplazo desde Excel")

    st.write(
        "Podés subir un Excel con columnas: Nombre, Apellido, DNI, Edad. "
        "Esto agrega los jugadores que no existan todavía."
    )

    archivo = st.file_uploader("Subir Excel inicial", type=["xlsx"])

    if archivo is not None:
        try:
            df_up = pd.read_excel(archivo)
            st.write("Vista previa:")
            st.dataframe(df_up.head(), use_container_width=True)

            if st.button("Importar Excel subido"):
                tmp_path = REPO_EXCEL_PATH
                asegurar_carpetas()
                with open(tmp_path, "wb") as f:
                    f.write(archivo.getbuffer())

                importar_excel_inicial(conn, tmp_path)
                exportar_jugadores_a_excel(conn)
                st.success("Importación completada.")
                st.rerun()
        except Exception as e:
            st.error(f"Error leyendo el archivo: {e}")


st.markdown("---")
st.caption("Archivos esperados: data/JUAGDORES_ESCUELA.xlsx y data/gestion_jugadores.db")
