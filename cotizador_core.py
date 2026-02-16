# cotizador_core.py – Welbe v3.1 (core para Streamlit)
# Basado en tu cotizador_welbe.py (misma lógica, sin Tkinter)

from __future__ import annotations
import itertools
import unicodedata
from pathlib import Path
from typing import List, Tuple, Dict
import pandas as pd

# ───────── Paths (compatibles con local y deploy) ─────────
BASE_DIR = Path(__file__).resolve().parent
ASSETS_DIR = BASE_DIR / "assets"
FILE_CHOPO = ASSETS_DIR / "Para Cotizar con base a Chopo.xlsx"
FILE_CP = ASSETS_DIR / "catalogo_cp.csv"

# ───────── Configuración ─────────
MARGIN_DEF = 0.33
FACTOR_FB_VOL = 2.00
FACTOR_FB_NOVOL = 2.20

MAIN_LAB = "CHOPO"
FACTOR_ZONA2 = 1.8  # Candidatos fallback: CHOPO × 1.8

# ✅ Etiqueta visible cuando el precio viene por fallback (base CHOPO × factor)
LAB_FALLBACK_LABEL = "AGREGAR RED"

# ✅ Columna amigable para usuario final (solo se llena cuando aplica fallback por batería incompleta)
OBS_COL = "Observación"

# ───────── Utilidades ─────────
def _clean(txt: str) -> str:
    return (
        "" if pd.isna(txt) else unicodedata.normalize("NFKD", str(txt))
        .encode("ascii", "ignore").decode()
        .strip().upper()
    )

def _fix_cp(s: pd.Series) -> pd.Series:
    return (
        s.astype(str).str.replace(r"\.0$", "", regex=True)
        .str.strip().str.zfill(5)
    )

def _read_xl(path: Path, sheet: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")
    return pd.read_excel(path, sheet_name=sheet)

# ───────── Carga de datos ─────────
def load_estudios() -> pd.DataFrame:
    df = _read_xl(FILE_CHOPO, "Estudios")
    df.columns = df.columns.str.upper().str.strip()
    df = df[["LABORATORIO", "NOMBRE AJUSTADO", "CATEGORIA LAB", "COSTO WELBE (SIN IVA)"]]
    df.columns = ["Laboratorio", "Estudio", "Categoria_lab", "Costo"]
    df["Laboratorio"] = df["Laboratorio"].apply(_clean)
    df["Estudio_norm"] = df["Estudio"].apply(_clean)
    df["Categoria_lab"] = df["Categoria_lab"].apply(_clean)
    return df.dropna(subset=["Estudio"])

def load_sucursales() -> pd.DataFrame:
    df = _read_xl(FILE_CHOPO, "Sucursales")
    df.columns = df.columns.str.upper().str.strip()

    # Base mínima (la tuya)
    base_cols = ["UNIDAD", "CODIGO POSTAL", "CATEGORIAS", "LABORATORIO"]

    # GEO opcional (solo si existen en el Excel)
    geo_cols = []
    for c in ["DELEGACION", "CIUDAD", "ESTADO"]:
        if c in df.columns:
            geo_cols.append(c)

    df = df[base_cols + geo_cols].copy()

    # Renombres
    rename_map = {
        "UNIDAD": "Sucursal",
        "CODIGO POSTAL": "CP",
        "CATEGORIAS": "Categorias",
        "LABORATORIO": "Laboratorio",
        "DELEGACION": "Delegacion",
        "CIUDAD": "Ciudad",
        "ESTADO": "Estado",
    }
    df = df.rename(columns=rename_map)

    df["CP"] = _fix_cp(df["CP"])
    df["Laboratorio"] = df["Laboratorio"].apply(_clean)
    df["Cats_set"] = df["Categorias"].fillna("").apply(
        lambda s: {_clean(c) for c in str(s).split(",") if str(c).strip()}
    )

    # Normaliza GEO si existe
    for c in ["Delegacion", "Ciudad", "Estado"]:
        if c in df.columns:
            df[c] = df[c].apply(_clean)

    return df.dropna(subset=["CP"])

def load_catalogo_cp() -> pd.DataFrame:
    if not FILE_CP.exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_CP}")

    df = pd.read_csv(FILE_CP, dtype=str, encoding="latin1")
    df.columns = df.columns.str.lower().str.strip()
    cp_col = next(c for c in ("d_codigo", "d_cp", "c_cp", "cp") if c in df.columns)

    # OJO: tu versión original llama "ciudad" a d_mnpio (municipio)
    df = df[[cp_col, "d_estado", "d_mnpio"]]
    df.columns = ["CP", "estado", "ciudad"]  # aquí ciudad = municipio (tu naming original)

    df["CP"] = _fix_cp(df["CP"])
    df["estado"] = df["estado"].apply(_clean)
    df["ciudad"] = df["ciudad"].apply(_clean)

    # Si existe d_ciudad en tu CSV, la guardamos como ciudad_real para el match por CIUDAD
    if "d_ciudad" in df.columns:
        df["ciudad_real"] = df["d_ciudad"].apply(_clean)
    else:
        # Intentamos leerla si venía en el CSV pero no la habías seleccionado
        try:
            df_full = pd.read_csv(FILE_CP, dtype=str, encoding="latin1")
            df_full.columns = df_full.columns.str.lower().str.strip()
            if "d_ciudad" in df_full.columns:
                df["ciudad_real"] = df_full["d_ciudad"].apply(_clean)
            else:
                df["ciudad_real"] = ""
        except Exception:
            df["ciudad_real"] = ""

    return df.dropna(subset=["CP", "ciudad"])

# ───────── Cobertura helpers ─────────
def cps_municipio(df_cp: pd.DataFrame, edo: str, ciu: str) -> List[str]:
    # ciu = municipio (por tu naming original)
    return df_cp.query("estado == @edo and ciudad == @ciu", engine="python")["CP"].tolist()

def _cat_ok_exact(cat: str, cats_series: pd.Series) -> bool:
    return any(cat == c for s in cats_series for c in s)

def _lab_cubre_todo(lab: str, df_est_req: pd.DataFrame, df_suc_sub: pd.DataFrame) -> bool:
    df_est_lab = df_est_req[df_est_req.Laboratorio == lab]
    df_suc_lab = df_suc_sub[df_suc_sub.Laboratorio == lab]
    for _, e in df_est_lab.iterrows():
        if not _cat_ok_exact(e.Categoria_lab, df_suc_lab["Cats_set"]):
            return False
    return True

def _labs_con_todo(df_est_req: pd.DataFrame, df_suc_sub: pd.DataFrame) -> List[str]:
    return [lab for lab in df_suc_sub["Laboratorio"].unique() if _lab_cubre_todo(lab, df_est_req, df_suc_sub)]

def _comb_dos_labs(df_est_req: pd.DataFrame, df_suc_sub: pd.DataFrame, est_norm: set) -> Tuple[str, str] | tuple:
    labs = df_suc_sub["Laboratorio"].unique()
    for lab1, lab2 in itertools.combinations(labs, 2):
        ok = True
        for estn in est_norm:
            r1 = df_est_req[(df_est_req.Estudio_norm == estn) & (df_est_req.Laboratorio == lab1)]
            r2 = df_est_req[(df_est_req.Estudio_norm == estn) & (df_est_req.Laboratorio == lab2)]
            if r1.empty and r2.empty:
                ok = False; break

            lab1_ok = (
                (not r1.empty) and
                _cat_ok_exact(r1.Categoria_lab.iloc[0], df_suc_sub[df_suc_sub.Laboratorio == lab1]["Cats_set"])
            )
            lab2_ok = (
                (not r2.empty) and
                _cat_ok_exact(r2.Categoria_lab.iloc[0], df_suc_sub[df_suc_sub.Laboratorio == lab2]["Cats_set"])
            )

            if not (lab1_ok or lab2_ok):
                ok = False; break

        if ok:
            return lab1, lab2
    return ()

def _observacion_bateria_incompleta(df_here: pd.DataFrame, df_est_req: pd.DataFrame, est_norm: set,
                                   studies_original: List[str], edo: str, ciu: str) -> str:
    """
    Devuelve un texto amigable para usuario final del tipo:
      "Mastografía no disponible en ningún laboratorio del municipio"
    """
    labs = sorted(df_here["Laboratorio"].unique().tolist())
    if not labs:
        return "Sin cobertura en el municipio"

    faltantes_globales: List[str] = []

    for est_name in studies_original:
        estn = _clean(est_name)
        disponible_en_alguno = False

        for lab in labs:
            df_lab_suc = df_here[df_here["Laboratorio"] == lab]
            if df_lab_suc.empty:
                continue

            r = df_est_req[(df_est_req["Laboratorio"] == lab) & (df_est_req["Estudio_norm"] == estn)]
            if r.empty:
                continue

            cat = r["Categoria_lab"].iloc[0]
            if _cat_ok_exact(cat, df_lab_suc["Cats_set"]):
                disponible_en_alguno = True
                break

        if not disponible_en_alguno:
            faltantes_globales.append(est_name)

    if not faltantes_globales:
        return "No hay laboratorio con batería completa en el municipio"

    principal = faltantes_globales[0]
    return f"{principal} no disponible en ningún laboratorio del municipio"


# ───────── NUEVO: Selección de sucursales con Delegación → Ciudad → fallback CP ─────────
def _sucursales_por_municipio(df_suc: pd.DataFrame, df_cp: pd.DataFrame, edo: str, muni: str) -> Tuple[pd.DataFrame, str]:
    """
    Flujo:
      1) Si Sucursales trae Estado+Delegacion: filtra por (Estado==edo AND Delegacion==muni)
      2) Si no hay y trae Estado+Ciudad: intenta match por Ciudad usando catalogo_cp.d_ciudad (ciudad_real)
      3) Si no hay: fallback original por CP (catalogo_cp por municipio)
    """
    edo_c = _clean(edo)
    muni_c = _clean(muni)

    # 1) Delegación
    if {"Estado", "Delegacion"}.issubset(df_suc.columns):
        df1 = df_suc[(df_suc["Estado"] == edo_c) & (df_suc["Delegacion"] == muni_c)]
        if not df1.empty:
            return df1, "delegacion"

    # 2) Ciudad
    if {"Estado", "Ciudad"}.issubset(df_suc.columns):
        # ciudades posibles del municipio desde el catálogo
        ciudades = (
            df_cp.query("estado == @edo_c and ciudad == @muni_c", engine="python")["ciudad_real"]
            .dropna().unique().tolist()
        )
        ciudades = [c for c in ciudades if str(c).strip()]

        for c in ciudades:
            c_c = _clean(c)
            df2 = df_suc[(df_suc["Estado"] == edo_c) & (df_suc["Ciudad"] == c_c)]
            if not df2.empty:
                return df2, "ciudad"

    # 3) fallback CP original
    cps = cps_municipio(df_cp, edo_c, muni_c)
    return df_suc[df_suc.CP.isin(cps)], "cp_fallback"


# ───────── COTIZACIÓN SENCILLA (Candidatos) ─────────
def armar_sencilla(sel_est: List[str], sel_ciu: List[Tuple[str, str]],
                   df_est: pd.DataFrame, df_suc: pd.DataFrame, df_cp: pd.DataFrame,
                   margin: float = MARGIN_DEF):

    if not sel_est or not sel_ciu:
        raise ValueError("Seleccione al menos un estudio y un municipio.")
    if margin >= 1:
        raise ValueError("El margen debe ser menor a 100%.")

    est_norm = {_clean(s) for s in sel_est}
    df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]

    chopo_map = dict(
        df_est[df_est.Laboratorio == MAIN_LAB][["Estudio_norm", "Costo"]].values
    )

    filas: List[Dict] = []

    for edo, muni in sel_ciu:
        # ✅ NUEVO: Delegación → Ciudad → CP fallback
        df_here, modo_geo = _sucursales_por_municipio(df_suc, df_cp, edo, muni)

        # Caso 1: sin sucursales → fallback directo CHOPO × 1.8
        if df_here.empty:
            for est_name in sel_est:
                estn = _clean(est_name)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    raise ValueError(f"No se encontró costo CHOPO para '{est_name}' en {muni}, {edo}.")
                costo = float(chopo_map[estn]) * FACTOR_ZONA2
                precio = round(costo / (1.0 - margin), 2)
                filas.append({
                    "Estado": edo,
                    "Municipio": muni,
                    "ModoGeo": modo_geo,
                    "Sucursal": "SIN SUCURSALES",
                    "Estudio": est_name,
                    "Costo": round(costo, 2),
                    "Precio": precio,
                    "Laboratorio": MAIN_LAB,
                    "Zona": "FALLBACK",
                })
            continue

        # Caso 2: buscar sucursales que cubran TODA la batería por lab
        labs_full: List[Tuple[str, str]] = []  # (lab, sucursal)

        for lab in sorted(df_here.Laboratorio.unique()):
            df_lab_suc = df_here[df_here.Laboratorio == lab]
            for _, suc_row in df_lab_suc.iterrows():
                cats = suc_row.Cats_set
                ok = True
                for estn in est_norm:
                    r = df_est_req[(df_est_req.Laboratorio == lab) & (df_est_req.Estudio_norm == estn)]
                    if r.empty:
                        ok = False; break
                    if r.Categoria_lab.iloc[0] not in cats:
                        ok = False; break
                if ok:
                    labs_full.append((lab, suc_row.Sucursal))
                    break

        # 2.a hay batería completa → listamos
        if labs_full:
            for lab, sucursal in labs_full:
                for est_name in sel_est:
                    estn = _clean(est_name)
                    r = df_est_req[(df_est_req.Laboratorio == lab) & (df_est_req.Estudio_norm == estn)]
                    if r.empty:
                        continue
                    costo = float(r.Costo.iloc[0])
                    precio = round(costo / (1.0 - margin), 2)
                    filas.append({
                        "Estado": edo,
                        "Municipio": muni,
                        "ModoGeo": modo_geo,
                        "Sucursal": sucursal,
                        "Estudio": est_name,
                        "Costo": round(costo, 2),
                        "Precio": precio,
                        "Laboratorio": lab,
                        "Zona": "DIRECTO",
                    })

        # 2.b no hay batería completa → fallback candidatos (CHOPO × 1.8)
        else:
            obs_txt = _observacion_bateria_incompleta(df_here, df_est_req, est_norm, sel_est, edo, muni)
            for est_name in sel_est:
                estn = _clean(est_name)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    raise ValueError(f"No se encontró costo CHOPO para '{est_name}' en {muni}, {edo}.")
                costo = float(chopo_map[estn]) * FACTOR_ZONA2
                precio = round(costo / (1.0 - margin), 2)
                filas.append({
                    "Estado": edo,
                    "Municipio": muni,
                    "ModoGeo": modo_geo,
                    "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": est_name,
                    "Costo": round(costo, 2),
                    "Precio": precio,
                    "Laboratorio": MAIN_LAB,
                    "Zona": "FALLBACK",
                    OBS_COL: obs_txt,
                })

    return pd.DataFrame(filas), {}


# ───────── COTIZACIÓN COMPUESTA (Periódicos) ─────────
def cotizar_compuesto(studies: List[str], ciudades: List[Tuple[str, str, int]],
                      df_est: pd.DataFrame, df_suc: pd.DataFrame, df_cp: pd.DataFrame,
                      margin: float = MARGIN_DEF, factor_fb: float = FACTOR_FB_VOL):

    if margin >= 1:
        raise ValueError("El margen debe ser menor a 100%.")

    has_vol = any((pers or 0) > 0 for _, _, pers in ciudades)
    has_no_vol = any((pers or 0) == 0 for _, _, pers in ciudades)
    if has_vol and has_no_vol:
        factor_global = FACTOR_FB_NOVOL
    else:
        factor_global = FACTOR_FB_VOL if has_vol else FACTOR_FB_NOVOL

    est_norm = {_clean(s) for s in studies}
    chopo_map = dict(df_est[df_est.Laboratorio == MAIN_LAB][["Estudio_norm", "Costo"]].values)

    rows_detalle: List[Dict] = []
    fallback_rows: List[Dict] = []

    for edo, muni, pers in ciudades:
        # ✅ NUEVO: Delegación → Ciudad → CP fallback
        df_here, modo_geo = _sucursales_por_municipio(df_suc, df_cp, edo, muni)
        df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]

        # 0) Sin sucursales → todo fallback (AGREGAR RED)
        if df_here.empty:
            for s in studies:
                estn = _clean(s)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    fallback_rows.append({
                        "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                        "Laboratorio": LAB_FALLBACK_LABEL, "Sucursal": "SIN SUCURSALES",
                        "Estudio": s, OBS_COL: "Sin sucursales en el municipio",
                        "Motivo": "Sin costo base para fallback"
                    })
                    continue

                costo = float(chopo_map[estn]) * factor_global
                precio = round(costo / (1.0 - margin), 2)
                rows_detalle.append({
                    "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                    "Laboratorio": LAB_FALLBACK_LABEL, "Sucursal": "SIN SUCURSALES",
                    "Estudio": s, "Costo_lab": round(costo, 2),
                    "Precio_lab": precio, "Margen": margin, "Fallback": True,
                    OBS_COL: "Sin sucursales en el municipio",
                })
                fallback_rows.append({
                    "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                    "Laboratorio": LAB_FALLBACK_LABEL, "Sucursal": "SIN SUCURSALES",
                    "Estudio": s, OBS_COL: "Sin sucursales en el municipio",
                    "Motivo": "Sin sucursales en municipio"
                })
            continue

        # 1) labs que cubran batería completa (por lab + sucursal)
        labs_full: List[Tuple[str, str]] = []
        for lab in sorted(df_here.Laboratorio.unique()):
            df_lab_suc = df_here[df_here.Laboratorio == lab]
            for _, suc_row in df_lab_suc.iterrows():
                cats = suc_row.Cats_set
                ok = True
                for estn in est_norm:
                    r = df_est_req[(df_est_req.Laboratorio == lab) & (df_est_req.Estudio_norm == estn)]
                    if r.empty:
                        ok = False; break
                    if r.Categoria_lab.iloc[0] not in cats:
                        ok = False; break
                if ok:
                    labs_full.append((lab, suc_row.Sucursal))
                    break

        # ✅ Si NO hay batería completa: NO mostramos labs parciales → fallback
        if not labs_full:
            obs_txt = _observacion_bateria_incompleta(df_here, df_est_req, est_norm, studies, edo, muni)
            for s in studies:
                estn = _clean(s)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    fallback_rows.append({
                        "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                        "Laboratorio": LAB_FALLBACK_LABEL, "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                        "Estudio": s, OBS_COL: obs_txt, "Motivo": "Sin costo base para fallback"
                    })
                    continue

                costo = float(chopo_map[estn]) * factor_global
                precio = round(costo / (1.0 - margin), 2)
                rows_detalle.append({
                    "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                    "Laboratorio": LAB_FALLBACK_LABEL, "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": s, "Costo_lab": round(costo, 2),
                    "Precio_lab": precio, "Margen": margin, "Fallback": True,
                    OBS_COL: obs_txt,
                })
                fallback_rows.append({
                    "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                    "Laboratorio": LAB_FALLBACK_LABEL, "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": s, OBS_COL: obs_txt,
                    "Motivo": "Ningún laboratorio cubre batería completa → fallback"
                })
            continue

        # 2) cotizar SOLO labs con batería completa
        for lab, sucursal in labs_full:
            df_suc_lab_suc = df_here[(df_here.Laboratorio == lab) & (df_here.Sucursal == sucursal)]
            suc_cats = df_suc_lab_suc["Cats_set"].iloc[0] if not df_suc_lab_suc.empty else set()

            for s in studies:
                estn = _clean(s)
                costo = None
                fallback_flag = False

                r = df_est_req[(df_est_req.Laboratorio == lab) & (df_est_req.Estudio_norm == estn)]
                if not r.empty:
                    cat = r.Categoria_lab.iloc[0]
                    if cat in suc_cats:
                        try:
                            costo = float(r.Costo.iloc[0])
                        except Exception:
                            costo = None

                # fallback por costo raro
                if costo is None and estn in chopo_map and pd.notna(chopo_map[estn]):
                    costo = float(chopo_map[estn]) * factor_global
                    fallback_flag = True

                if costo is None:
                    fallback_rows.append({
                        "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                        "Laboratorio": lab, "Sucursal": sucursal,
                        "Estudio": s, OBS_COL: "", "Motivo": "Sin costo disponible"
                    })
                    continue

                precio = round(costo / (1.0 - margin), 2)
                rows_detalle.append({
                    "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                    "Laboratorio": (LAB_FALLBACK_LABEL if fallback_flag else lab),
                    "Sucursal": sucursal,
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": fallback_flag,
                    OBS_COL: (f"{s} cotizado por fallback" if fallback_flag else ""),
                })

                if fallback_flag:
                    fallback_rows.append({
                        "Estado": edo, "Municipio": muni, "ModoGeo": modo_geo,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": sucursal,
                        "Estudio": s,
                        OBS_COL: f"{s} cotizado por fallback",
                        "Motivo": "Fallback (base CHOPO × factor)"
                    })

    return pd.DataFrame(rows_detalle), pd.DataFrame(fallback_rows)


# ───────── Helper para “Labs recomendados por municipio” (tu lógica del tab) ─────────
def recomendar_labs_por_municipio(df_est: pd.DataFrame, df_suc: pd.DataFrame, df_cp: pd.DataFrame,
                                  estudios: List[str], municipios: List[Tuple[str, str]]) -> pd.DataFrame:
    est_norm = {_clean(s) for s in estudios}
    df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]
    rows = []

    for edo, muni in municipios:
        df_here, modo_geo = _sucursales_por_municipio(df_suc, df_cp, edo, muni)

        if df_here.empty:
            rows.append({"Estado": edo, "Municipio": muni, "ModoGeo": modo_geo, "Recomendados": "—", "Nota": "Sin cobertura"})
            continue

        nota = ""
        if (MAIN_LAB in df_here.Laboratorio.values) and _lab_cubre_todo(MAIN_LAB, df_est_req, df_here):
            recomendados = [MAIN_LAB]
        else:
            todo = _labs_con_todo(df_est_req, df_here)
            if todo:
                lab_eleg = min(todo, key=lambda l: df_est_req[df_est_req.Laboratorio == l].Costo.sum())
                recomendados = [lab_eleg]
            else:
                combo = _comb_dos_labs(df_est_req, df_here, est_norm)
                recomendados = list(combo) if combo else []
                if recomendados:
                    nota = "Combinación de 2 laboratorios"

        rows.append({
            "Estado": edo,
            "Municipio": muni,
            "ModoGeo": modo_geo,
            "Recomendados": "; ".join(recomendados) if recomendados else "— (usar fallback por estudio)",
            "Nota": nota
        })

    return pd.DataFrame(rows)


# ───────── Loader maestro (para cache en Streamlit) ─────────
def cargar_todo() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_est = load_estudios()
    df_suc = load_sucursales()
    df_cp = load_catalogo_cp()
    return df_est, df_suc, df_cp
