# cotizador_core.py – Welbe v3.1 + Nueva lógica Delegación → Ciudad → fallback original (CP)
from __future__ import annotations

import itertools
import unicodedata
from pathlib import Path
from typing import List, Tuple, Dict, Optional

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
FACTOR_ZONA2 = 1.8  # fallback: base CHOPO × 1.8

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
    """
    Sheet: 'Estudios'
    Columnas esperadas:
      LABORATORIO, NOMBRE AJUSTADO, CATEGORIA LAB, COSTO WELBE (SIN IVA)
    """
    df = _read_xl(FILE_CHOPO, "Estudios")
    df.columns = df.columns.str.upper().str.strip()

    df = df[["LABORATORIO", "NOMBRE AJUSTADO", "CATEGORIA LAB", "COSTO WELBE (SIN IVA)"]]
    df.columns = ["Laboratorio", "Estudio", "Categoria_lab", "Costo"]

    df["Laboratorio"] = df["Laboratorio"].apply(_clean)
    df["Estudio_norm"] = df["Estudio"].apply(_clean)
    df["Categoria_lab"] = df["Categoria_lab"].apply(_clean)

    return df.dropna(subset=["Estudio"])

def load_sucursales() -> pd.DataFrame:
    """
    LÓGICA ORIGINAL (por CP):
    Sheet: 'Sucursales'
    Columnas:
      UNIDAD, CODIGO POSTAL, CATEGORIAS, LABORATORIO
    """
    df = _read_xl(FILE_CHOPO, "Sucursales")
    df.columns = df.columns.str.upper().str.strip()

    df = df[["UNIDAD", "CODIGO POSTAL", "CATEGORIAS", "LABORATORIO"]]
    df.columns = ["Sucursal", "CP", "Categorias", "Laboratorio"]

    df["CP"] = _fix_cp(df["CP"])
    df["Laboratorio"] = df["Laboratorio"].apply(_clean)
    df["Cats_set"] = df["Categorias"].fillna("").apply(
        lambda s: {_clean(c) for c in str(s).split(",") if str(c).strip()}
    )
    return df.dropna(subset=["CP"])

def load_sucursales_geo() -> pd.DataFrame:
    """
    NUEVA FUENTE (GEO):
    Detecta automáticamente el sheet que contenga 'para' y 'chopo' en el nombre.
    Columnas esperadas:
      UNIDAD, CODIGO POSTAL, CATEGORIAS, LABORATORIO, DELEGACION, CIUDAD, ESTADO
    """
    if not FILE_CHOPO.exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_CHOPO}")

    xls = pd.ExcelFile(FILE_CHOPO)
    sheets = xls.sheet_names

    def _match(name: str) -> bool:
        n = name.lower()
        return ("para" in n) and ("chopo" in n)

    candidatos = [s for s in sheets if _match(s)]
    if not candidatos:
        raise ValueError(
            "No encontré ningún sheet GEO tipo 'Para cotizar ... Chopo'. "
            f"Sheets disponibles: {sheets}"
        )

    sheet = candidatos[0]
    df = pd.read_excel(FILE_CHOPO, sheet_name=sheet)
    df.columns = df.columns.str.upper().str.strip()

    needed = ["UNIDAD", "CODIGO POSTAL", "CATEGORIAS", "LABORATORIO", "DELEGACION", "CIUDAD", "ESTADO"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(
            f"Faltan columnas en sheet '{sheet}': {missing}. "
            f"Columnas reales: {list(df.columns)}"
        )

    df = df[["UNIDAD", "CODIGO POSTAL", "CATEGORIAS", "LABORATORIO", "DELEGACION", "CIUDAD", "ESTADO"]]
    df.columns = ["Sucursal", "CP", "Categorias", "Laboratorio", "Delegacion", "Ciudad", "Estado"]

    df["CP"] = _fix_cp(df["CP"])
    df["Laboratorio"] = df["Laboratorio"].apply(_clean)

    df["Estado"] = df["Estado"].apply(_clean)
    df["Delegacion"] = df["Delegacion"].apply(_clean)
    df["Ciudad"] = df["Ciudad"].apply(_clean)

    df["Cats_set"] = df["Categorias"].fillna("").apply(
        lambda s: {_clean(c) for c in str(s).split(",") if str(c).strip()}
    )

    return df.dropna(subset=["CP", "Estado", "Delegacion"])

def load_catalogo_cp() -> pd.DataFrame:
    """
    Catalogo_cp (csv):
      d_codigo, d_estado, d_mnpio (viene como D_mnpio), d_ciudad (opcional)
    """
    if not FILE_CP.exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_CP}")

    df = pd.read_csv(FILE_CP, dtype=str, encoding="latin1")
    df.columns = df.columns.str.lower().str.strip()

    cp_col = next(c for c in ("d_codigo", "d_cp", "c_cp", "cp") if c in df.columns)

    keep = [cp_col, "d_estado", "d_mnpio"]
    if "d_ciudad" in df.columns:
        keep.append("d_ciudad")

    df = df[keep].copy()
    df = df.rename(columns={cp_col: "CP", "d_estado": "estado", "d_mnpio": "municipio"})
    if "d_ciudad" in df.columns:
        df = df.rename(columns={"d_ciudad": "ciudad"})
    else:
        df["ciudad"] = None

    df["CP"] = _fix_cp(df["CP"])
    df["estado"] = df["estado"].apply(_clean)
    df["municipio"] = df["municipio"].apply(_clean)
    df["ciudad"] = df["ciudad"].apply(_clean)

    return df.dropna(subset=["CP", "municipio", "estado"])

# ───────── Cobertura helpers ─────────
def cps_municipio(df_cp: pd.DataFrame, edo: str, mnp: str) -> List[str]:
    return (
        df_cp.query("estado == @edo and municipio == @mnp", engine="python")["CP"]
        .dropna()
        .tolist()
    )

def ciudades_por_municipio(df_cp: pd.DataFrame, edo: str, mnp: str) -> List[str]:
    if "ciudad" not in df_cp.columns:
        return []
    out = (
        df_cp.query("estado == @edo and municipio == @mnp", engine="python")["ciudad"]
        .dropna()
        .unique()
        .tolist()
    )
    return out

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

            lab1_ok = (not r1.empty and _cat_ok_exact(r1.Categoria_lab.iloc[0], df_suc_sub[df_suc_sub.Laboratorio == lab1]["Cats_set"]))
            lab2_ok = (not r2.empty and _cat_ok_exact(r2.Categoria_lab.iloc[0], df_suc_sub[df_suc_sub.Laboratorio == lab2]["Cats_set"]))
            if not (lab1_ok or lab2_ok):
                ok = False; break
        if ok:
            return lab1, lab2
    return ()

def _observacion_bateria_incompleta(df_here: pd.DataFrame, df_est_req: pd.DataFrame, est_norm: set, studies_original: List[str]) -> str:
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

# ───────── NUEVO: GEO lookup (Delegación → Ciudad → fallback por CP) ─────────
def _df_here_por_geo(edo: str, mnp: str, df_suc_geo: pd.DataFrame, df_cp: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    edo_c = _clean(edo)
    mnp_c = _clean(mnp)

    # 1) Delegación exacta
    m1 = df_suc_geo[(df_suc_geo["Estado"] == edo_c) & (df_suc_geo["Delegacion"] == mnp_c)]
    if not m1.empty:
        return m1, "delegacion"

    # 2) Ciudad (derivada de catalogo_cp)
    for c in ciudades_por_municipio(df_cp, edo_c, mnp_c):
        c_c = _clean(c)
        m2 = df_suc_geo[(df_suc_geo["Estado"] == edo_c) & (df_suc_geo["Ciudad"] == c_c)]
        if not m2.empty:
            return m2, "ciudad"

    return pd.DataFrame(), "fallback"

def _df_here_final(
    edo: str,
    mnp: str,
    df_suc_geo: pd.DataFrame,
    df_suc_cp: pd.DataFrame,
    df_cp: pd.DataFrame
) -> Tuple[pd.DataFrame, str]:
    df_geo, modo = _df_here_por_geo(edo, mnp, df_suc_geo, df_cp)
    if modo != "fallback":
        return df_geo, modo

    edo_c, mnp_c = _clean(edo), _clean(mnp)
    cps = cps_municipio(df_cp, edo_c, mnp_c)
    return df_suc_cp[df_suc_cp.CP.isin(cps)], "fallback"

# ───────── COTIZACIÓN SENCILLA ─────────
def armar_sencilla(
    sel_est: List[str],
    sel_ciu: List[Tuple[str, str]],  # (Estado, Municipio/Delegación)
    df_est: pd.DataFrame,
    df_suc: pd.DataFrame,
    df_cp: pd.DataFrame,
    df_suc_geo: Optional[pd.DataFrame] = None,
    margin: float = MARGIN_DEF
):
    if not sel_est or not sel_ciu:
        raise ValueError("Seleccione al menos un estudio y un municipio.")
    if margin >= 1:
        raise ValueError("El margen debe ser menor a 100%.")

    est_norm = {_clean(s) for s in sel_est}
    df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]
    chopo_map = dict(df_est[df_est.Laboratorio == MAIN_LAB][["Estudio_norm", "Costo"]].values)

    filas: List[Dict] = []

    for edo, mnp in sel_ciu:
        # GEO → fallback CP
        if df_suc_geo is not None:
            df_here, modo_geo = _df_here_final(edo, mnp, df_suc_geo, df_suc, df_cp)
        else:
            edo_c, mnp_c = _clean(edo), _clean(mnp)
            cps = cps_municipio(df_cp, edo_c, mnp_c)
            df_here = df_suc[df_suc.CP.isin(cps)]
            modo_geo = "fallback"

        # Caso 1: sin sucursales → fallback directo
        if df_here.empty:
            for est_name in sel_est:
                estn = _clean(est_name)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    raise ValueError(f"No se encontró costo CHOPO para '{est_name}' en {mnp}, {edo}.")
                costo = float(chopo_map[estn]) * FACTOR_ZONA2
                precio = round(costo / (1.0 - margin), 2)
                filas.append({
                    "Estado": edo, "Municipio": mnp,
                    "Sucursal": "SIN SUCURSALES",
                    "Estudio": est_name,
                    "Costo": round(costo, 2),
                    "Precio": precio,
                    "Laboratorio": MAIN_LAB,
                    "Zona": "FALLBACK",
                    "ModoGeo": modo_geo,
                })
            continue

        # Caso 2: buscar sucursal que cubra batería completa por lab
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
                        "Estado": edo, "Municipio": mnp,
                        "Sucursal": sucursal,
                        "Estudio": est_name,
                        "Costo": round(costo, 2),
                        "Precio": precio,
                        "Laboratorio": lab,
                        "Zona": "DIRECTO",
                        "ModoGeo": modo_geo,
                    })
        else:
            for est_name in sel_est:
                estn = _clean(est_name)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    raise ValueError(f"No se encontró costo CHOPO para '{est_name}' en {mnp}, {edo}.")
                costo = float(chopo_map[estn]) * FACTOR_ZONA2
                precio = round(costo / (1.0 - margin), 2)
                filas.append({
                    "Estado": edo, "Municipio": mnp,
                    "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": est_name,
                    "Costo": round(costo, 2),
                    "Precio": precio,
                    "Laboratorio": MAIN_LAB,
                    "Zona": "FALLBACK",
                    "ModoGeo": modo_geo,
                })

    return pd.DataFrame(filas), {}

# ───────── COTIZACIÓN COMPUESTA ─────────
def cotizar_compuesto(
    studies: List[str],
    ciudades: List[Tuple[str, str, int]],  # (Estado, Municipio/Delegación, personas)
    df_est: pd.DataFrame,
    df_suc: pd.DataFrame,
    df_cp: pd.DataFrame,
    df_suc_geo: Optional[pd.DataFrame] = None,
    margin: float = MARGIN_DEF,
    factor_fb: float = FACTOR_FB_VOL
):
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

    for edo, mnp, pers in ciudades:
        # GEO → fallback CP
        if df_suc_geo is not None:
            df_here, modo_geo = _df_here_final(edo, mnp, df_suc_geo, df_suc, df_cp)
        else:
            edo_c, mnp_c = _clean(edo), _clean(mnp)
            cps = cps_municipio(df_cp, edo_c, mnp_c)
            df_here = df_suc[df_suc.CP.isin(cps)]
            modo_geo = "fallback"

        df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]

        # Sin sucursales → todo fallback
        if df_here.empty:
            for s in studies:
                estn = _clean(s)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    fallback_rows.append({
                        "Estado": edo, "Municipio": mnp,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": "SIN SUCURSALES",
                        "Estudio": s,
                        OBS_COL: "Sin sucursales en el municipio",
                        "Motivo": "Sin costo base para fallback",
                        "ModoGeo": modo_geo,
                    })
                    continue

                costo = float(chopo_map[estn]) * factor_global
                precio = round(costo / (1.0 - margin), 2)

                rows_detalle.append({
                    "Estado": edo, "Municipio": mnp,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSALES",
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": True,
                    OBS_COL: "Sin sucursales en el municipio",
                    "ModoGeo": modo_geo,
                })
                fallback_rows.append({
                    "Estado": edo, "Municipio": mnp,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSALES",
                    "Estudio": s,
                    OBS_COL: "Sin sucursales en el municipio",
                    "Motivo": "Sin sucursales en municipio",
                    "ModoGeo": modo_geo,
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
                    r = df_est_req[(df_est_req.Laboratorio == lab) & (df_est_req.Estudio_norm == estn)]
                    if r.empty:
                        ok = False; break
                    if r.Categoria_lab.iloc[0] not in cats:
                        ok = False; break
                if ok:
                    labs_full.append((lab, suc_row.Sucursal))
                    break

        if not labs_full:
            obs_txt = _observacion_bateria_incompleta(df_here, df_est_req, est_norm, studies)
            for s in studies:
                estn = _clean(s)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    fallback_rows.append({
                        "Estado": edo, "Municipio": mnp,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                        "Estudio": s,
                        OBS_COL: obs_txt,
                        "Motivo": "Sin costo base para fallback",
                        "ModoGeo": modo_geo,
                    })
                    continue

                costo = float(chopo_map[estn]) * factor_global
                precio = round(costo / (1.0 - margin), 2)

                rows_detalle.append({
                    "Estado": edo, "Municipio": mnp,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": True,
                    OBS_COL: obs_txt,
                    "ModoGeo": modo_geo,
                })
                fallback_rows.append({
                    "Estado": edo, "Municipio": mnp,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": s,
                    OBS_COL: obs_txt,
                    "Motivo": "Ningún laboratorio cubre batería completa → fallback",
                    "ModoGeo": modo_geo,
                })
            continue

        # cotizar SOLO labs con batería completa
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

                if costo is None and estn in chopo_map and pd.notna(chopo_map[estn]):
                    costo = float(chopo_map[estn]) * factor_global
                    fallback_flag = True

                if costo is None:
                    fallback_rows.append({
                        "Estado": edo, "Municipio": mnp,
                        "Laboratorio": lab,
                        "Sucursal": sucursal,
                        "Estudio": s,
                        OBS_COL: "",
                        "Motivo": "Sin costo disponible",
                        "ModoGeo": modo_geo,
                    })
                    continue

                precio = round(costo / (1.0 - margin), 2)
                rows_detalle.append({
                    "Estado": edo, "Municipio": mnp,
                    "Laboratorio": (LAB_FALLBACK_LABEL if fallback_flag else lab),
                    "Sucursal": sucursal,
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": fallback_flag,
                    OBS_COL: (f"{s} cotizado por fallback" if fallback_flag else ""),
                    "ModoGeo": modo_geo,
                })
                if fallback_flag:
                    fallback_rows.append({
                        "Estado": edo, "Municipio": mnp,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": sucursal,
                        "Estudio": s,
                        OBS_COL: f"{s} cotizado por fallback",
                        "Motivo": "Fallback (base CHOPO × factor)",
                        "ModoGeo": modo_geo,
                    })

    return pd.DataFrame(rows_detalle), pd.DataFrame(fallback_rows)

# ───────── Labs recomendados por municipio ─────────
def recomendar_labs_por_municipio(
    df_est: pd.DataFrame,
    df_suc: pd.DataFrame,
    df_cp: pd.DataFrame,
    estudios: List[str],
    municipios: List[Tuple[str, str]],
    df_suc_geo: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    est_norm = {_clean(s) for s in estudios}
    df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]

    rows = []
    for edo, mnp in municipios:
        if df_suc_geo is not None:
            df_here, modo_geo = _df_here_final(edo, mnp, df_suc_geo, df_suc, df_cp)
        else:
            edo_c, mnp_c = _clean(edo), _clean(mnp)
            cps = cps_municipio(df_cp, edo_c, mnp_c)
            df_here = df_suc[df_suc.CP.isin(cps)]
            modo_geo = "fallback"

        if df_here.empty:
            rows.append({"Estado": edo, "Municipio": mnp, "Recomendados": "—", "Nota": "Sin cobertura", "ModoGeo": modo_geo})
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
            "Municipio": mnp,
            "Recomendados": "; ".join(recomendados) if recomendados else "— (usar fallback por estudio)",
            "Nota": nota,
            "ModoGeo": modo_geo,
        })

    return pd.DataFrame(rows)

# ───────── Loader maestro (para cache en Streamlit) ─────────
def cargar_todo() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_est = load_estudios()
    df_suc = load_sucursales()          # original por CP
    df_cp = load_catalogo_cp()
    df_suc_geo = load_sucursales_geo()  # delegación / ciudad
    return df_est, df_suc, df_cp, df_suc_geo
