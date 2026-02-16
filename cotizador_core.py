# cotizador_core.py – Welbe v3.1 (core para Streamlit)
from __future__ import annotations

import itertools
import unicodedata
from pathlib import Path
from typing import List, Tuple, Dict
import pandas as pd

# ───────── Paths ─────────
BASE_DIR = Path(__file__).resolve().parent
ASSETS_DIR = BASE_DIR / "assets"
FILE_CHOPO = ASSETS_DIR / "Para Cotizar con base a Chopo.xlsx"
FILE_CP = ASSETS_DIR / "catalogo_cp.csv"

# ───────── Configuración ─────────
MARGIN_DEF = 0.33
FACTOR_FB_VOL = 2.00
FACTOR_FB_NOVOL = 2.20

MAIN_LAB = "CHOPO"
FACTOR_ZONA2 = 1.8  # fallback (base CHOPO × 1.8)

LAB_FALLBACK_LABEL = "AGREGAR RED"
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
        s.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(5)
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

    base_cols = ["UNIDAD", "CODIGO POSTAL", "CATEGORIAS", "LABORATORIO"]
    geo_cols = [c for c in ["DELEGACION", "CIUDAD", "ESTADO"] if c in df.columns]

    df = df[base_cols + geo_cols].copy()

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

    for c in ["Delegacion", "Ciudad", "Estado"]:
        if c in df.columns:
            df[c] = df[c].apply(_clean)

    return df.dropna(subset=["CP"])

def load_catalogo_cp() -> pd.DataFrame:
    """
    catalogo_cp.csv (según tu estructura):
      - CP: d_codigo   ✅
      - Estado: d_estado
      - Municipio: d_mnpio
    """
    if not FILE_CP.exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_CP}")

    df = pd.read_csv(FILE_CP, dtype=str, encoding="latin1")
    df.columns = df.columns.str.lower().str.strip()

    # CP real es d_codigo, pero lo dejamos robusto por si cambia
    cp_col = None
    for c in ("d_codigo", "d_cp", "c_cp", "cp"):
        if c in df.columns:
            cp_col = c
            break
    if not cp_col:
        raise ValueError(f"No encontré columna de CP. Columnas disponibles: {list(df.columns)}")

    needed = [cp_col, "d_estado", "d_mnpio"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en catalogo_cp.csv: {missing}. Columnas reales: {list(df.columns)}")

    df = df[[cp_col, "d_estado", "d_mnpio"]].copy()
    df.columns = ["CP", "estado", "municipio"]

    df["CP"] = _fix_cp(df["CP"])
    df["estado"] = df["estado"].apply(_clean)
    df["municipio"] = df["municipio"].apply(_clean)

    # ✅ Compatibilidad: por si tu app vieja pedía df_cp["ciudad"]
    df["ciudad"] = df["municipio"]

    return df.dropna(subset=["CP", "estado", "municipio"])


# ───────── Cobertura helpers ─────────
def cps_municipio(df_cp: pd.DataFrame, edo: str, muni: str) -> List[str]:
    return df_cp.query("estado == @edo and municipio == @muni", engine="python")["CP"].tolist()

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
                ok = False
                break

            lab1_ok = (not r1.empty) and _cat_ok_exact(
                r1.Categoria_lab.iloc[0],
                df_suc_sub[df_suc_sub.Laboratorio == lab1]["Cats_set"]
            )
            lab2_ok = (not r2.empty) and _cat_ok_exact(
                r2.Categoria_lab.iloc[0],
                df_suc_sub[df_suc_sub.Laboratorio == lab2]["Cats_set"]
            )

            if not (lab1_ok or lab2_ok):
                ok = False
                break

        if ok:
            return lab1, lab2
    return ()

def _observacion_bateria_incompleta(df_here: pd.DataFrame, df_est_req: pd.DataFrame, est_norm: set,
                                   studies_original: List[str], edo: str, muni: str) -> str:
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

    return f"{faltantes_globales[0]} no disponible en ningún laboratorio del municipio"


# ───────── Resolver GEO: CP → Delegación → Ciudad ─────────
def _sucursales_por_municipio(df_suc: pd.DataFrame, df_cp: pd.DataFrame, edo: str, muni: str) -> tuple[pd.DataFrame, str]:
    edo_c = _clean(edo)
    muni_c = _clean(muni)

    # 1) CP primero
    cps = cps_municipio(df_cp, edo_c, muni_c)
    df_cp_match = df_suc[df_suc.CP.isin(cps)]
    if not df_cp_match.empty:
        return df_cp_match, "cp"

    # 2) Delegación
    if {"Estado", "Delegacion"}.issubset(df_suc.columns):
        df_del = df_suc[(df_suc["Estado"] == edo_c) & (df_suc["Delegacion"] == muni_c)]
        if not df_del.empty:
            return df_del, "delegacion"

    # 3) Ciudad
    if {"Estado", "Ciudad"}.issubset(df_suc.columns):
        df_ciu = df_suc[(df_suc["Estado"] == edo_c) & (df_suc["Ciudad"] == muni_c)]
        if not df_ciu.empty:
            return df_ciu, "ciudad"

    return df_suc.iloc[0:0], "sin_match"


# ───────── COTIZACIÓN SENCILLA ─────────
def armar_sencilla(sel_est: List[str], sel_muni: List[Tuple[str, str]],
                   df_est: pd.DataFrame, df_suc: pd.DataFrame, df_cp: pd.DataFrame,
                   margin: float = MARGIN_DEF):

    if not sel_est or not sel_muni:
        raise ValueError("Seleccione al menos un estudio y un municipio.")
    if margin >= 1:
        raise ValueError("El margen debe ser menor a 100%.")

    est_norm = {_clean(s) for s in sel_est}
    df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]

    chopo_map = dict(df_est[df_est.Laboratorio == MAIN_LAB][["Estudio_norm", "Costo"]].values)

    filas: List[Dict] = []

    for edo, muni in sel_muni:
        df_here, modo_geo = _sucursales_por_municipio(df_suc, df_cp, edo, muni)

        # Sin sucursales → fallback
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

        # Buscar batería completa por lab+sucursal
        labs_full: List[Tuple[str, str]] = []
        for lab in sorted(df_here.Laboratorio.unique()):
            df_lab_suc = df_here[df_here.Laboratorio == lab]
            for _, suc_row in df_lab_suc.iterrows():
                cats = suc_row.Cats_set
                ok = True
                for estn in est_norm:
                    r = df_est_req[(df_est_req.Laboratorio == lab) & (df_est_req.Estudio_norm == estn)]
                    if r.empty:
                        ok = False
                        break
                    if r.Categoria_lab.iloc[0] not in cats:
                        ok = False
                        break
                if ok:
                    labs_full.append((lab, suc_row.Sucursal))
                    break

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


# ───────── COTIZACIÓN COMPUESTA ─────────
def cotizar_compuesto(studies: List[str], municipios: List[Tuple[str, str, int]],
                      df_est: pd.DataFrame, df_suc: pd.DataFrame, df_cp: pd.DataFrame,
                      margin: float = MARGIN_DEF, factor_fb: float = FACTOR_FB_VOL):

    if margin >= 1:
        raise ValueError("El margen debe ser menor a 100%.")

    has_vol = any((pers or 0) > 0 for _, _, pers in municipios)
    has_no_vol = any((pers or 0) == 0 for _, _, pers in municipios)

    if has_vol and has_no_vol:
        factor_global = FACTOR_FB_NOVOL
    else:
        factor_global = FACTOR_FB_VOL if has_vol else FACTOR_FB_NOVOL

    est_norm = {_clean(s) for s in studies}
    df_est_req_all = df_est[df_est.Estudio_norm.isin(est_norm)]
    chopo_map = dict(df_est[df_est.Laboratorio == MAIN_LAB][["Estudio_norm", "Costo"]].values)

    rows_detalle: List[Dict] = []
    fallback_rows: List[Dict] = []

    for edo, muni, pers in municipios:
        df_here, modo_geo = _sucursales_por_municipio(df_suc, df_cp, edo, muni)

        # Sin sucursales → fallback total
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
                    "Estudio": s, "Costo_lab": round(costo, 2), "Precio_lab": precio,
                    "Margen": margin, "Fallback": True, "Personas": pers,
                    OBS_COL: "Sin sucursales en el municipio",
                })
            continue

        # labs con batería completa
        labs_full: List[Tuple[str, str]] = []
        for lab in sorted(df_here.Laboratorio.unique()):
            df_lab_suc = df_here[df_here.Laboratorio == lab]
            for _, suc_row in df_lab_suc.iterrows():
                cats = suc_row.Cats_set
                ok = True
                for estn in est_norm:
                    r = df_est_req_all[(df_est_req_all.Laboratorio == lab) & (df_est_req_all.Estudio_norm == estn)]
                    if r.empty or (r.Categoria_lab.iloc[0] not in cats):
                        ok = False
                        break
                if ok:
                    labs_full.append((lab, suc_row.Sucursal))
                    break

        if not labs_full:
            obs_txt = _observacion_bateria_incompleta(df_here, df_est_req_all, est_norm, studies, edo, muni)
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
                    "Estudio": s, "Costo_lab": round(costo, 2), "Precio_lab": precio,
                    "Margen": margin, "Fallback": True, "Personas": pers,
                    OBS_COL: obs_txt,
                })
            continue

        # cotizar labs completos
        for lab, sucursal in labs_full:
            df_suc_lab_suc = df_here[(df_here.Laboratorio == lab) & (df_here.Sucursal == sucursal)]
            suc_cats = df_suc_lab_suc["Cats_set"].iloc[0] if not df_suc_lab_suc.empty else set()

            for s in studies:
                estn = _clean(s)
                costo = None
                fallback_flag = False

                r = df_est_req_all[(df_est_req_all.Laboratorio == lab) & (df_est_req_all.Estudio_norm == estn)]
                if not r.empty and r.Categoria_lab.iloc[0] in suc_cats:
                    try:
                        costo = float(r.Costo.iloc[0])
                    except Exception:
                        costo = None

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
                    "Personas": pers,
                    OBS_COL: (f"{s} cotizado por fallback" if fallback_flag else ""),
                })

    return pd.DataFrame(rows_detalle), pd.DataFrame(fallback_rows)


# ───────── Helper: Labs recomendados ─────────
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


# ───────── Loader maestro ─────────
def cargar_todo() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_est = load_estudios()
    df_suc = load_sucursales()
    df_cp = load_catalogo_cp()
    return df_est, df_suc, df_cp
