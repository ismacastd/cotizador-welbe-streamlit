# cotizador_core.py  – Welbe v3.1.1 (core para Streamlit)
# ✅ Mantiene tu lógica de categorías/batería/fallback intacta
# ✅ Mantiene zfill(5) en CP (NO se cambia)
# ✅ Nuevo: entrada a sucursales por capas usando (Estado, Municipio):
#    1) CP vía SEPOMEX (igual que antes)
#    2) Si falla: Estado+Ciudad == Municipio
#    3) Si falla: Estado+Delegacion == Municipio
#    4) Si falla: match "contiene" (suave) dentro del Estado

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
FILE_CP    = ASSETS_DIR / "catalogo_cp.csv"

# ───────── Configuración ─────────
MARGIN_DEF      = 0.33
FACTOR_FB_VOL   = 2.00
FACTOR_FB_NOVOL = 2.20

MAIN_LAB = "CHOPO"
FACTOR_ZONA2 = 1.8  # Candidatos fallback: CHOPO × 1.8

# ✅ Etiqueta visible cuando el precio viene por fallback (base CHOPO × factor)
LAB_FALLBACK_LABEL = "AGREGAR RED"

# ✅ Columna amigable para usuario final (solo se llena cuando aplica fallback por batería incompleta)
OBS_COL = "Observación"

# ───────── Utilidades ─────────
def _clean(txt: str) -> str:
    return ("" if pd.isna(txt) else
            unicodedata.normalize("NFKD", str(txt))
            .encode("ascii", "ignore").decode()
            .strip().upper())

def _fix_cp(s: pd.Series) -> pd.Series:
    # ✅ Mantener zfill(5) como definiste
    return (s.astype(str).str.replace(r"\.0$", "", regex=True)
                   .str.strip().str.zfill(5))

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
    df["Laboratorio"]   = df["Laboratorio"].apply(_clean)
    df["Estudio_norm"]  = df["Estudio"].apply(_clean)
    df["Categoria_lab"] = df["Categoria_lab"].apply(_clean)
    return df.dropna(subset=["Estudio"])

def load_sucursales() -> pd.DataFrame:
    """
    Ahora cargamos Ciudad/Estado/Delegacion además de CP para permitir match alternativo
    cuando CP (SEPOMEX) no encuentre sucursales.
    """
    df = _read_xl(FILE_CHOPO, "Sucursales")
    df.columns = df.columns.str.upper().str.strip()

    # Encabezados según tu captura:
    # ... CODIGO POSTAL, DELEGACION, CIUDAD, ESTADO, CATEGORIAS, LABORATORIO, UNIDAD ...
    needed = ["UNIDAD", "CODIGO POSTAL", "CATEGORIAS", "LABORATORIO", "DELEGACION", "CIUDAD", "ESTADO"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en hoja 'Sucursales': {missing}")

    df = df[needed]
    df.columns = ["Sucursal", "CP", "Categorias", "Laboratorio", "Delegacion", "Ciudad", "Estado"]

    df["CP"]          = _fix_cp(df["CP"])
    df["Laboratorio"] = df["Laboratorio"].apply(_clean)
    df["Delegacion"]  = df["Delegacion"].apply(_clean)
    df["Ciudad"]      = df["Ciudad"].apply(_clean)
    df["Estado"]      = df["Estado"].apply(_clean)

    df["Cats_set"] = df["Categorias"].fillna("").apply(
        lambda s: {_clean(c) for c in str(s).split(",") if str(c).strip()}
    )

    return df.dropna(subset=["Sucursal"])

def load_catalogo_cp() -> pd.DataFrame:
    if not FILE_CP.exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_CP}")
    df = pd.read_csv(FILE_CP, dtype=str, encoding="latin1")
    df.columns = df.columns.str.lower().str.strip()
    cp_col = next(c for c in ("d_codigo", "d_cp", "c_cp", "cp") if c in df.columns)

    df = df[[cp_col, "d_estado", "d_mnpio"]]
    df.columns = ["CP", "estado", "municipio"]

    df["CP"]        = _fix_cp(df["CP"])
    df["estado"]    = df["estado"].apply(_clean)
    df["municipio"] = df["municipio"].apply(_clean)

    return df.dropna(subset=["CP", "municipio"])

# ───────── Entrada a sucursales por municipio (capas) ─────────
def cps_municipio(df_cp: pd.DataFrame, edo: str, mun: str) -> List[str]:
    return df_cp.query("estado == @edo and municipio == @mun", engine="python")["CP"].tolist()

def _contains_either(a: str, b: str) -> bool:
    # "fuzzy suave": A contiene B o B contiene A, ya limpios
    if not a or not b:
        return False
    return (a in b) or (b in a)

def sucursales_para_municipio(df_suc: pd.DataFrame, df_cp: pd.DataFrame, edo: str, mun: str) -> pd.DataFrame:
    """
    Dado (Estado, Municipio) ya normalizados, regresa sucursales candidatas en este orden:
      1) Match por CP (SEPOMEX municipio -> lista CP -> sucursales CP IN lista)
      2) Match por Estado + Ciudad == Municipio
      3) Match por Estado + Delegacion == Municipio
      4) Match por Estado + (Ciudad contiene Municipio o viceversa) OR (Delegacion contiene Municipio o viceversa)
    """
    # 1) CP exacto (como antes)
    cps = cps_municipio(df_cp, edo, mun)
    if cps:
        df_cp_match = df_suc[df_suc["CP"].isin(cps)]
        if not df_cp_match.empty:
            return df_cp_match

    # 2) Ciudad exacta
    df_city = df_suc[(df_suc["Estado"] == edo) & (df_suc["Ciudad"] == mun)]
    if not df_city.empty:
        return df_city

    # 3) Delegación exacta
    df_del = df_suc[(df_suc["Estado"] == edo) & (df_suc["Delegacion"] == mun)]
    if not df_del.empty:
        return df_del

    # 4) Contiene (suave) dentro del Estado
    df_state = df_suc[df_suc["Estado"] == edo].copy()
    if df_state.empty:
        return df_state

    mask = df_state.apply(
        lambda r: _contains_either(r.get("Ciudad", ""), mun) or _contains_either(r.get("Delegacion", ""), mun),
        axis=1
    )
    df_fuzzy = df_state[mask]
    return df_fuzzy

# ───────── Cobertura helpers ─────────
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
    return [lab for lab in df_suc_sub["Laboratorio"].unique()
            if _lab_cubre_todo(lab, df_est_req, df_suc_sub)]

def _comb_dos_labs(df_est_req: pd.DataFrame, df_suc_sub: pd.DataFrame, est_norm: set) -> Tuple[str, str] | tuple:
    labs = df_suc_sub["Laboratorio"].unique()
    for lab1, lab2 in itertools.combinations(labs, 2):
        ok = True
        for estn in est_norm:
            r1 = df_est_req[(df_est_req.Estudio_norm == estn) & (df_est_req.Laboratorio == lab1)]
            r2 = df_est_req[(df_est_req.Estudio_norm == estn) & (df_est_req.Laboratorio == lab2)]
            if r1.empty and r2.empty:
                ok = False; break

            lab1_ok = (not r1.empty and _cat_ok_exact(r1.Categoria_lab.iloc[0],
                                                     df_suc_sub[df_suc_sub.Laboratorio == lab1]["Cats_set"]))
            lab2_ok = (not r2.empty and _cat_ok_exact(r2.Categoria_lab.iloc[0],
                                                     df_suc_sub[df_suc_sub.Laboratorio == lab2]["Cats_set"]))
            if not (lab1_ok or lab2_ok):
                ok = False; break
        if ok:
            return lab1, lab2
    return ()

def _observacion_bateria_incompleta(df_here: pd.DataFrame, df_est_req: pd.DataFrame, est_norm: set,
                                   studies_original: List[str], edo: str, mun: str) -> str:
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

    for edo, mun in sel_ciu:
        edo_c, mun_c = _clean(edo), _clean(mun)

        # ✅ NUEVO: entrada por capas (CP -> Ciudad -> Delegación -> contiene)
        df_here = sucursales_para_municipio(df_suc, df_cp, edo_c, mun_c)

        # Caso 1: sin sucursales → fallback directo CHOPO × 1.8
        if df_here.empty:
            for est_name in sel_est:
                estn = _clean(est_name)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    raise ValueError(f"No se encontró costo CHOPO para '{est_name}' en {mun}, {edo}.")
                costo = float(chopo_map[estn]) * FACTOR_ZONA2
                precio = round(costo / (1.0 - margin), 2)
                filas.append({
                    "Estado": edo, "Ciudad": mun,
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
                        "Estado": edo, "Ciudad": mun,
                        "Sucursal": sucursal,
                        "Estudio": est_name,
                        "Costo": round(costo, 2),
                        "Precio": precio,
                        "Laboratorio": lab,
                        "Zona": "DIRECTO",
                    })
        # 2.b no hay batería completa → fallback candidatos (CHOPO × 1.8)
        else:
            for est_name in sel_est:
                estn = _clean(est_name)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    raise ValueError(f"No se encontró costo CHOPO para '{est_name}' en {mun}, {edo}.")
                costo = float(chopo_map[estn]) * FACTOR_ZONA2
                precio = round(costo / (1.0 - margin), 2)
                filas.append({
                    "Estado": edo, "Ciudad": mun,
                    "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": est_name,
                    "Costo": round(costo, 2),
                    "Precio": precio,
                    "Laboratorio": MAIN_LAB,
                    "Zona": "FALLBACK",
                })

    return pd.DataFrame(filas), {}

# ───────── COTIZACIÓN COMPUESTA (Periódicos) ─────────
def cotizar_compuesto(studies: List[str],
                      ciudades: List[Tuple[str, str, int]],
                      df_est: pd.DataFrame, df_suc: pd.DataFrame, df_cp: pd.DataFrame,
                      margin: float = MARGIN_DEF,
                      factor_fb: float = FACTOR_FB_VOL):
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

    for edo, mun, pers in ciudades:
        edo_c, mun_c = _clean(edo), _clean(mun)

        # ✅ NUEVO: entrada por capas
        df_here = sucursales_para_municipio(df_suc, df_cp, edo_c, mun_c)
        df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]

        # 0) Sin sucursales → todo fallback (AGREGAR RED)
        if df_here.empty:
            for s in studies:
                estn = _clean(s)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    fallback_rows.append({
                        "Estado": edo, "Municipio": mun,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": "SIN SUCURSALES",
                        "Estudio": s,
                        OBS_COL: "Sin sucursales en el municipio",
                        "Motivo": "Sin costo base para fallback"
                    })
                    continue

                costo = float(chopo_map[estn]) * factor_global
                precio = round(costo / (1.0 - margin), 2)

                rows_detalle.append({
                    "Estado": edo, "Municipio": mun,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSALES",
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": True,
                    OBS_COL: "Sin sucursales en el municipio",
                })
                fallback_rows.append({
                    "Estado": edo, "Municipio": mun,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSALES",
                    "Estudio": s,
                    OBS_COL: "Sin sucursales en el municipio",
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

        # ✅ Si NO hay batería completa: NO mostramos labs parciales.
        if not labs_full:
            obs_txt = _observacion_bateria_incompleta(df_here, df_est_req, est_norm, studies, edo, mun)
            for s in studies:
                estn = _clean(s)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    fallback_rows.append({
                        "Estado": edo, "Municipio": mun,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                        "Estudio": s,
                        OBS_COL: obs_txt,
                        "Motivo": "Sin costo base para fallback"
                    })
                    continue

                costo = float(chopo_map[estn]) * factor_global
                precio = round(costo / (1.0 - margin), 2)

                rows_detalle.append({
                    "Estado": edo, "Municipio": mun,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": True,
                    OBS_COL: obs_txt,
                })
                fallback_rows.append({
                    "Estado": edo, "Municipio": mun,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": s,
                    OBS_COL: obs_txt,
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

                if costo is None and estn in chopo_map and pd.notna(chopo_map[estn]):
                    costo = float(chopo_map[estn]) * factor_global
                    fallback_flag = True

                if costo is None:
                    fallback_rows.append({
                        "Estado": edo, "Municipio": mun,
                        "Laboratorio": lab, "Sucursal": sucursal,
                        "Estudio": s,
                        OBS_COL: "",
                        "Motivo": "Sin costo disponible"
                    })
                    continue

                precio = round(costo / (1.0 - margin), 2)
                rows_detalle.append({
                    "Estado": edo, "Municipio": mun,
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
                        "Estado": edo, "Municipio": mun,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": sucursal,
                        "Estudio": s,
                        OBS_COL: f"{s} cotizado por fallback",
                        "Motivo": "Fallback (base CHOPO × factor)"
                    })

    return pd.DataFrame(rows_detalle), pd.DataFrame(fallback_rows)

# ───────── Helper para “Labs recomendados por municipio” ─────────
def recomendar_labs_por_municipio(df_est: pd.DataFrame, df_suc: pd.DataFrame, df_cp: pd.DataFrame,
                                 estudios: List[str], municipios: List[Tuple[str, str]]) -> pd.DataFrame:
    est_norm = {_clean(s) for s in estudios}
    df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]

    rows = []
    for edo, mun in municipios:
        edo_c, mun_c = _clean(edo), _clean(mun)

        # ✅ NUEVO: entrada por capas
        df_here = sucursales_para_municipio(df_suc, df_cp, edo_c, mun_c)

        if df_here.empty:
            rows.append({"Estado": edo, "Municipio": mun, "Recomendados": "—", "Nota": "Sin cobertura"})
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
            "Municipio": mun,
            "Recomendados": "; ".join(recomendados) if recomendados else "— (usar fallback por estudio)",
            "Nota": nota
        })

    return pd.DataFrame(rows)

# ───────── Loader maestro (para cache en Streamlit) ─────────
def cargar_todo() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_est = load_estudios()
    df_suc = load_sucursales()
    df_cp  = load_catalogo_cp()
    return df_est, df_suc, df_cp
