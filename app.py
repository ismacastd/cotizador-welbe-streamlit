import streamlit as st
import pandas as pd
from io import BytesIO
import traceback

from cotizador_core import (
    cargar_todo,
    cotizar_compuesto,
    recomendar_labs_por_municipio,
    MARGIN_DEF
)

st.set_page_config(page_title="Cotizador Welbe — Periódicos", layout="wide")
st.title("Cotizador Welbe — 2026 V1.1")

@st.cache_data
def _load_data():
    return cargar_todo()

def _to_excel_bytes(sheets: dict) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name[:31])
    return bio.getvalue()

def _safe_int(x) -> int:
    try:
        if x is None:
            return 0
        if isinstance(x, str) and x.strip() == "":
            return 0
        if pd.isna(x):
            return 0
        return int(float(x))
    except Exception:
        return 0

# ───────── Cargar catálogos ─────────
try:
    df_est, df_suc, df_cp = _load_data()
except Exception as e:
    st.error(f"Error cargando archivos en /assets:\n\n{e}")
    st.code(traceback.format_exc())
    st.stop()

# ───────── Normalizar columnas esperadas en df_cp ─────────
df_cp.columns = df_cp.columns.str.lower().str.strip()

if "estado" not in df_cp.columns:
    st.error(f"catalogo_cp no trae columna 'estado'. Columnas: {list(df_cp.columns)}")
    st.stop()

geo_col = "municipio" if "municipio" in df_cp.columns else ("ciudad" if "ciudad" in df_cp.columns else None)
if not geo_col:
    st.error(f"catalogo_cp no trae columna 'municipio' ni 'ciudad'. Columnas: {list(df_cp.columns)}")
    st.stop()

# ───────── Sidebar ─────────
st.sidebar.header("Parámetros")

margen_pct = st.sidebar.number_input(
    "Margen %",
    min_value=0.0,
    max_value=99.0,
    value=float(MARGIN_DEF * 100),
    step=0.5
)
margen = float(margen_pct) / 100.0

st.sidebar.divider()

# ───────── Selección de estudios ─────────
estudios = sorted(df_est["Estudio"].dropna().unique().tolist())
sel_est = st.sidebar.multiselect("Estudios", options=estudios)

# ───────── Selección de municipios (Estado + Municipio) ─────────
estados = sorted(df_cp["estado"].dropna().unique().tolist())
sel_estado = st.sidebar.selectbox("Estado", options=[""] + estados)

municipios_opts = []
if sel_estado:
    municipios_opts = sorted(
        df_cp[df_cp["estado"] == sel_estado][geo_col].dropna().unique().tolist()
    )

sel_muni = st.sidebar.selectbox("Ciudad/Municipio", options=[""] + municipios_opts)

if "municipios" not in st.session_state:
    st.session_state["municipios"] = []

col_add1, col_add2 = st.sidebar.columns([1, 1])
if col_add1.button("Agregar municipio"):
    if sel_estado and sel_muni:
        actuales = st.session_state["municipios"]
        if not any(m["Estado"] == sel_estado and m["Municipio"] == sel_muni for m in actuales):
            actuales.append({"Estado": sel_estado, "Municipio": sel_muni, "Personas": 0})
        else:
            st.sidebar.info("Ese municipio ya está en la lista.")
    else:
        st.sidebar.warning("Selecciona Estado y Municipio.")
if col_add2.button("Limpiar lista"):
    st.session_state["municipios"] = []

st.sidebar.caption("Tip: agrega varios municipios y edita Personas (volumen) para el cálculo de Periódicos.")

# ───────── Editor de municipios ─────────
st.subheader("Municipios seleccionados")
mun_df = pd.DataFrame(st.session_state["municipios"])
if mun_df.empty:
    mun_df = pd.DataFrame(columns=["Estado", "Municipio", "Personas"])

mun_df = st.data_editor(
    mun_df,
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "Estado": st.column_config.TextColumn(required=True),
        "Municipio": st.column_config.TextColumn(required=True),
        "Personas": st.column_config.NumberColumn(min_value=0, step=1, help="0 = sin volumen"),
    },
    key="mun_editor"
)

st.session_state["municipios"] = mun_df.to_dict(orient="records")

st.divider()

# ───────── Botón calcular ─────────
if st.button("CALCULAR", type="primary"):
    if not sel_est:
        st.error("Selecciona al menos 1 estudio.")
        st.stop()
    if mun_df.empty:
        st.error("Agrega al menos 1 municipio.")
        st.stop()

    # 🔒 Construcción ultra segura de listas
    municipios_simple = []
    municipios_comp = []

    for _, r in mun_df.iterrows():
        edo = str(r.get("Estado", "") or "").strip()
        muni = str(r.get("Municipio", "") or "").strip()
        pers = _safe_int(r.get("Personas", 0))

        if not edo or not muni:
            continue

        municipios_simple.append((edo, muni))
        municipios_comp.append((edo, muni, pers))

    if not municipios_comp:
        st.error("Tu lista tiene filas vacías. Asegúrate de que Estado y Municipio no estén en blanco.")
        st.stop()

    # DEBUG visible
    with st.expander("Debug: Municipios enviados al core"):
        st.write("municipios_comp:", municipios_comp)
        st.write("estudios:", sel_est)

    with st.spinner("Calculando Periódicos..."):
        try:
            df_det, df_fb = cotizar_compuesto(
                studies=list(sel_est),
                ciudades=municipios_comp,
                df_est=df_est,
                df_suc=df_suc,
                df_cp=df_cp,
                margin=margen
            )
        except Exception as e:
            st.error("Tronó el cálculo. Aquí va el error REAL (ya sin redacción):")
            st.write(str(e))
            st.code(traceback.format_exc())
            st.stop()

    st.success("Listo (Periódicos).")

    tab1, tab2 = st.tabs(["Cotización", "Labs x Municipio"])

    with tab1:
        st.subheader("Cotización (detalle)")
        st.dataframe(df_det, use_container_width=True)

        if df_fb is not None and not df_fb.empty:
            st.warning(f"Fallback detectado: {len(df_fb)} fila(s). Revisa la pestaña Fallback en el Excel.")

    with tab2:
        st.subheader("Labs recomendados por municipio (resumen)")
        try:
            df_rec = recomendar_labs_por_municipio(df_est, df_suc, df_cp, list(sel_est), municipios_simple)
            st.dataframe(df_rec, use_container_width=True)
        except Exception as e:
            st.error("Error calculando labs recomendados:")
            st.write(str(e))
            st.code(traceback.format_exc())

    sheets = {"Cotizacion": df_det}
    if df_fb is not None and not df_fb.empty:
        sheets["Fallback"] = df_fb

    try:
        sheets["Labs_x_Municipio"] = recomendar_labs_por_municipio(df_est, df_suc, df_cp, list(sel_est), municipios_simple)
    except Exception:
        pass

    excel_bytes = _to_excel_bytes(sheets)
    st.download_button(
        label="Descargar Excel",
        data=excel_bytes,
        file_name="Cotizacion_Periodicos.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

