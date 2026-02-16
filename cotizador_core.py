from __future__ import annotations

import itertools
import unicodedata
from pathlib import Path
from typing import List, Tuple, Dict, Any

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
FACTOR_ZONA2 = 1.8  # candidatos fallback: CHOPO × 1.8

LAB_FALLBACK_LABEL = "AGREGAR RED"
OBS_COL = "Observación"


# ───────── Utilidades ─────────
def _clean(txt: str) -> str:
    return (
        "" if pd.isna(txt)
        else unicodedata.normalize("NFKD", str(txt))
        .encode("ascii", "ignore")
        .decode()
        .strip()
        .upper()
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

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cols = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in cols:
            return cols[key]
    raise KeyError(f"No encontré ninguna columna de {candidates}. Columnas disponibles: {list(df.columns)}")


# ───────── Carga de datos ─────────
def load_estudios() -> pd.DataFrame:
    df = _read_xl(FILE_CHOPO, "Estudios")
    df.columns = df.columns.str.upper().str.strip()

    df = df[["LABORATORIO", "NOMBRE AJUSTADO", "CATEGORIA LAB", "COSTO WELBE (SIN IVA)"]].copy()
    df.columns = ["Laboratorio", "Estudio", "Categoria_lab", "Costo"]

    df["Laboratorio"] = df["Laboratorio"].apply(_clean)
    df["Estudio_norm"] = df["Estudio"].apply(_clean)
    df["Categoria_lab"] = df["Categoria_lab"].apply(_clean)

    return df.dropna(subset=["Estudio"])


def load_sucursales() -> pd.DataFrame:
    df = _read_xl(FILE_CHOPO, "Sucursales")
    df.columns = df.columns.str.upper().str.strip()

    # Requeridas para la lógica:
    base_cols = ["UNIDAD", "CODIGO POSTAL", "CATEGORIAS", "LABORATORIO"]
    for c in base_cols:
        if c not in df.columns:
            raise KeyError(f"En hoja 'Sucursales' falta columna requerida: {c}. Columnas: {list(df.columns)}")

    # Opcionales para GEO fallback:
    # (pueden no existir y no pasa nada)
    opt_map = {}
    if "DELEGACION" in df.columns:
        opt_map["DELEGACION"] = "DELEGACION"
    if "DELEGACIÓN" in df.columns:
        opt_map["DELEGACIÓN"] = "DELEGACION"
    if "CIUDAD" in df.columns:
        opt_map["CIUDAD"] = "CIUDAD"
    if "ESTADO" in df.columns:
        opt_map["ESTADO"] = "ESTADO"

    keep = base_cols + list(opt_map.keys())
    df = df[keep].copy()

    df.rename(
        columns={
            "UNIDAD": "Sucursal",
            "CODIGO POSTAL": "CP",
            "CATEGORIAS": "Categorias",
            "LABORATORIO": "Laboratorio",
            **opt_map,
        },
        inplace=True,
    )

    df["CP"] = _fix_cp(df["CP"])
    df["Laboratorio"] = df["Laboratorio"].apply(_clean)

    # Normalizar GEO opcional
    df["Delegacion_norm"] = df["DELEGACION"].apply(_clean) if "DELEGACION" in df.columns else ""
    df["Ciudad_norm"] = df["CIUDAD"].apply(_clean) if "CIUDAD" in df.columns else ""
    df["Estado_norm"] = df["ESTADO"].apply(_clean) if "ESTADO" in df.columns else ""

    # Cats_set por sucursal
    df["Cats_set"] = df["Categorias"].fillna("").apply(
        lambda s: {_clean(c) for c in str(s).split(",") if str(c).strip()}
    )

    return df.dropna(subset=["CP"])


def load_catalogo_cp() -> pd.DataFrame:
    """
    Devuelve SIEMPRE: CP, estado, municipio
    CP viene de d_codigo (tu aclaración), pero soporta variantes.
    """
    if not FILE_CP.exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_CP}")

    df = pd.read_csv(FILE_CP, dtype=str, encoding="latin1")
    df.columns = df.columns.str.lower().str.strip()

    cp_col = "d_codigo" if "d_codigo" in df.columns else _pick_col(df, ["d_codigo", "d_cp", "c_cp", "cp"])
    edo_col = _pick_col(df, ["d_estado", "estado"])
    muni_col = _pick_col(df, ["d_mnpio", "d_municipio", "municipio", "d_ciudad", "ciudad"])

    out = df[[cp_col, edo_col, muni_col]].copy()
    out.columns = ["CP", "estado", "municipio"]

    out["CP"] = _fix_cp(out["CP"])
    out["estado"] = out["estado"].apply(_clean)
    out["municipio"] = out["municipio"].apply(_clean)

    return out.dropna(subset=["CP", "estado", "municipio"])


# ───────── Cobertura helpers ─────────
def cps_municipio(df_cp: pd.DataFrame, edo: str, muni: str) -> List[str]:
    """
    Robusto aunque df_cp venga crudo o estandarizado.
    """
    cols = {c.lower().strip(): c for c in df_cp.columns}

    # Si ya viene estandarizado:
    if "cp" in cols and "estado" in cols and "municipio" in cols:
        cp_c = cols["cp"]
        edo_c = cols["estado"]
        muni_c = cols["municipio"]
        q = df_cp.query(f"{edo_c} == @edo and {muni_c} == @muni", engine="python")
        return q[cp_c].dropna().astype(str).str.zfill(5).tolist()

    # Si viene crudo:
    cp_col = cols.get("d_codigo") or cols.get("d_cp") or cols.get("c_cp") or cols.get("cp")
    edo_col = cols.get("d_estado") or cols.get("estado")
    muni_col = cols.get("d_mnpio") or cols.get("municipio") or cols.get("d_ciudad") or cols.get("ciudad")

    if not (cp_col and edo_col and muni_col):
        raise KeyError(f"Catalogo CP sin columnas esperadas. Columnas: {list(df_cp.columns)}")

    tmp = df_cp[[cp_col, edo_col, muni_col]].copy()
    tmp.columns = ["CP", "estado", "municipio"]
    tmp["CP"] = _fix_cp(tmp["CP"])
    tmp["estado"] = tmp["estado"].apply(_clean)
    tmp["municipio"] = tmp["municipio"].apply(_clean)

    return tmp.query("estado == @edo and municipio == @muni", engine="python")["CP"].tolist()


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
            lab1_ok = (
                (not r1.empty)
                and _cat_ok_exact(r1.Categoria_lab.iloc[0], df_suc_sub[df_suc_sub.Laboratorio == lab1]["Cats_set"])
            )
            lab2_ok = (
                (not r2.empty)
                and _cat_ok_exact(r2.Categoria_lab.iloc[0], df_suc_sub[df_suc_sub.Laboratorio == lab2]["Cats_set"])
            )
            if not (lab1_ok or lab2_ok):
                ok = False
                break
        if ok:
            return lab1, lab2
    return ()

def _observacion_bateria_incompleta(df_here: pd.DataFrame, df_est_req: pd.DataFrame, studies_original: List[str]) -> str:
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


# ───────── GEO: CP -> Delegación -> Ciudad ─────────
def _subset_sucursales_por_geo(df_suc: pd.DataFrame, df_cp: pd.DataFrame, edo: str, muni: str) -> pd.DataFrame:
    edo_c, muni_c = _clean(edo), _clean(muni)

    # 1) CPs del municipio
    cps = cps_municipio(df_cp, edo_c, muni_c)
    df_here = df_suc[df_suc.CP.isin(cps)].copy()
    if not df_here.empty:
        df_here["GeoFuente"] = "CP"
        return df_here

    # 2) Delegación
    if "Delegacion_norm" in df_suc.columns and "Estado_norm" in df_suc.columns:
        df_here = df_suc[(df_suc["Estado_norm"] == edo_c) & (df_suc["Delegacion_norm"] == muni_c)].copy()
        if not df_here.empty:
            df_here["GeoFuente"] = "DELEGACION"
            return df_here

    # 3) Ciudad
    if "Ciudad_norm" in df_suc.columns and "Estado_norm" in df_suc.columns:
        df_here = df_suc[(df_suc["Estado_norm"] == edo_c) & (df_suc["Ciudad_norm"] == muni_c)].copy()
        if not df_here.empty:
            df_here["GeoFuente"] = "CIUDAD"
            return df_here

    return df_suc.iloc[0:0].copy()


# ───────── COTIZACIÓN COMPUESTA (Periódicos) ─────────
def cotizar_compuesto(
    studies: List[str],
    ciudades: List[Tuple[str, str, int]] | None = None,
    df_est: pd.DataFrame | None = None,
    df_suc: pd.DataFrame | None = None,
    df_cp: pd.DataFrame | None = None,
    margin: float = MARGIN_DEF,
    factor_fb: float = FACTOR_FB_VOL,
    **kwargs: Any
):
    """
    Compatibilidad:
    - app.py puede mandar ciudades=...
    - o municipios=... (alias)
    """
    if ciudades is None:
        ciudades = kwargs.get("municipios") or kwargs.get("cities") or []
    if "ciudades" in kwargs and not ciudades:
        ciudades = kwargs["ciudades"]

    if df_est is None or df_suc is None or df_cp is None:
        raise ValueError("df_est, df_suc, df_cp son requeridos")

    if margin >= 1:
        raise ValueError("El margen debe ser menor a 100%.")

    has_vol = any((pers or 0) > 0 for _, _, pers in ciudades)
    has_no_vol = any((pers or 0) == 0 for _, _, pers in ciudades)

    if has_vol and has_no_vol:
        factor_global = FACTOR_FB_NOVOL
    else:
        factor_global = FACTOR_FB_VOL if has_vol else FACTOR_FB_NOVOL

    est_norm = {_clean(s) for s in studies}
    df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)].copy()

    chopo_map = dict(df_est[df_est.Laboratorio == MAIN_LAB][["Estudio_norm", "Costo"]].values)

    rows_detalle: List[Dict] = []
    fallback_rows: List[Dict] = []

    for edo, muni, pers in ciudades:
        edo_raw, muni_raw = str(edo), str(muni)
        df_here = _subset_sucursales_por_geo(df_suc, df_cp, edo_raw, muni_raw)

        # 0) Sin sucursales → fallback
        if df_here.empty:
            for s in studies:
                estn = _clean(s)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    fallback_rows.append({
                        "Estado": edo_raw, "Municipio": muni_raw,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": "SIN SUCURSALES",
                        "Estudio": s,
                        "GeoFuente": "N/A",
                        OBS_COL: "Sin sucursales (CP/Delegación/Ciudad)",
                        "Motivo": "Sin costo base para fallback"
                    })
                    continue

                costo = float(chopo_map[estn]) * factor_global
                precio = round(costo / (1.0 - margin), 2)

                rows_detalle.append({
                    "Estado": edo_raw, "Municipio": muni_raw,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSALES",
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": True,
                    "GeoFuente": "N/A",
                    OBS_COL: "Sin sucursales (CP/Delegación/Ciudad)",
                })
            continue

        # 1) labs con batería completa (por lab + sucursal)
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

        geo_fuente = df_here["GeoFuente"].iloc[0] if "GeoFuente" in df_here.columns and not df_here.empty else ""

        # Si NO hay batería completa → fallback AGREGAR RED
        if not labs_full:
            obs_txt = _observacion_bateria_incompleta(df_here, df_est_req, studies)

            for s in studies:
                estn = _clean(s)
                if estn not in chopo_map or pd.isna(chopo_map[estn]):
                    fallback_rows.append({
                        "Estado": edo_raw, "Municipio": muni_raw,
                        "Laboratorio": LAB_FALLBACK_LABEL,
                        "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                        "Estudio": s,
                        "GeoFuente": geo_fuente,
                        OBS_COL: obs_txt,
                        "Motivo": "Sin costo base para fallback"
                    })
                    continue

                costo = float(chopo_map[estn]) * factor_global
                precio = round(costo / (1.0 - margin), 2)

                rows_detalle.append({
                    "Estado": edo_raw, "Municipio": muni_raw,
                    "Laboratorio": LAB_FALLBACK_LABEL,
                    "Sucursal": "SIN SUCURSAL CON BATERÍA COMPLETA",
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": True,
                    "GeoFuente": geo_fuente,
                    OBS_COL: obs_txt,
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
                        "Estado": edo_raw, "Municipio": muni_raw,
                        "Laboratorio": lab,
                        "Sucursal": sucursal,
                        "Estudio": s,
                        "GeoFuente": geo_fuente,
                        OBS_COL: "",
                        "Motivo": "Sin costo disponible"
                    })
                    continue

                precio = round(costo / (1.0 - margin), 2)

                rows_detalle.append({
                    "Estado": edo_raw, "Municipio": muni_raw,
                    "Laboratorio": (LAB_FALLBACK_LABEL if fallback_flag else lab),
                    "Sucursal": sucursal,
                    "Estudio": s,
                    "Costo_lab": round(costo, 2),
                    "Precio_lab": precio,
                    "Margen": margin,
                    "Fallback": fallback_flag,
                    "GeoFuente": geo_fuente,
                    OBS_COL: (f"{s} cotizado por fallback" if fallback_flag else ""),
                })

    return pd.DataFrame(rows_detalle), pd.DataFrame(fallback_rows)


# ───────── Helper: Labs recomendados por municipio ─────────
def recomendar_labs_por_municipio(
    df_est: pd.DataFrame,
    df_suc: pd.DataFrame,
    df_cp: pd.DataFrame,
    estudios: List[str],
    municipios: List[Tuple[str, str]]
) -> pd.DataFrame:
    est_norm = {_clean(s) for s in estudios}
    df_est_req = df_est[df_est.Estudio_norm.isin(est_norm)]

    rows = []
    for edo, muni in municipios:
        df_here = _subset_sucursales_por_geo(df_suc, df_cp, edo, muni)

        if df_here.empty:
            rows.append({"Estado": edo, "Municipio": muni, "Recomendados": "—", "Nota": "Sin cobertura", "GeoFuente": ""})
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
            "Recomendados": "; ".join(recomendados) if recomendados else "— (usar fallback por estudio)",
            "Nota": nota,
            "GeoFuente": df_here["GeoFuente"].iloc[0] if "GeoFuente" in df_here.columns and not df_here.empty else "",
        })

    return pd.DataFrame(rows)


# ───────── Loader maestro ─────────
def cargar_todo() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_est = load_estudios()
    df_suc = load_sucursales()
    df_cp = load_catalogo_cp()
    return df_est, df_suc, df_cp
