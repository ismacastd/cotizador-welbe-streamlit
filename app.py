import streamlit as st
import pandas as pd
from io import BytesIO

from cotizador_core import (
    cargar_todo,
    cotizar_compuesto,
    recomendar_labs_por_municipio,
    MARGIN_DEF
)

st.set_page_config(page_title="Cotizador Welbe — Periódicos", layout="wide")
st.title("Cotizador Welbe — Periódicos (Compuesta) — Diciembre 2026")

@st.cache_data
def _load_data():
    return cargar_todo()

def _to_excel_bytes(sheets: dict) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name[:31])
    return bio.getvalue()

# ───────── Cargar catálogos ─────────
try:
    df_est, df_suc, df_cp = _load_data()
except Exception as e:
    st.error(f"Error cargando archivos en /assets:\n\n{e}")
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

# ───────── Selección de municipios (Estado + Ciudad) ─────────
estados = sorted(df_cp["estado"].dropna().unique().tolist())
sel_estado = st.sidebar.selectbox("Estado", options=[""] + estados)

ciudades = []
if sel_estado:
    ciudades = sorted(df_cp[df_cp["estado"] == sel_estado]["ciudad"].dropna().unique().tolist())

sel_ciudad = st.sidebar.selectbox("Ciudad/Municipio", options=[""] + ciudades)

if "municipios" not in st.session_state:
    st.session_state["municipios"] = []  # lista de dicts

col_add1, col_add2 = st.sidebar.columns([1, 1])
if col_add1.button("Agregar municipio"):
    if sel_estado and sel_ciudad:
        # Evitar duplicados
        actuales = st.session_state["municipios"]
        if not any(m["Estado"] == sel_estado and m["Municipio"] == sel_ciudad for m in actuales):
            actuales.append({"Estado": sel_estado, "Municipio": sel_ciudad, "Personas": 0})
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

# Guardar lo editado
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

    municipios_simple = [(r["Estado"], r["Municipio"]) for _, r in mun_df.iterrows()]
    municipios_comp = [(r["Estado"], r["Municipio"], int(r.get("Personas", 0) or 0)) for _, r in mun_df.iterrows()]

    with st.spinner("Calculando Periódicos..."):
        df_det, df_fb = cotizar_compuesto(
            studies=sel_est,
            ciudades=municipios_comp,
            df_est=df_est,
            df_suc=df_suc,
            df_cp=df_cp,
            margin=margen
        )

    st.success("Listo (Periódicos).")

    tab1, tab2 = st.tabs(["Cotización", "Labs x Municipio"])

    with tab1:
        st.subheader("Cotización (detalle)")
        st.dataframe(df_det, use_container_width=True)

        if df_fb is not None and not df_fb.empty:
            st.warning(f"Fallback detectado: {len(df_fb)} fila(s). Revisa la pestaña Fallback en el Excel.")

    with tab2:
        st.subheader("Labs recomendados por municipio (resumen)")
        df_rec = recomendar_labs_por_municipio(df_est, df_suc, df_cp, sel_est, municipios_simple)
        st.dataframe(df_rec, use_container_width=True)

    sheets = {"Cotizacion": df_det}
    if df_fb is not None and not df_fb.empty:
        sheets["Fallback"] = df_fb
    sheets["Labs_x_Municipio"] = recomendar_labs_por_municipio(df_est, df_suc, df_cp, sel_est, municipios_simple)

    excel_bytes = _to_excel_bytes(sheets)
    st.download_button(
        label="Descargar Excel",
        data=excel_bytes,
        file_name="Cotizacion_Periodicos.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )