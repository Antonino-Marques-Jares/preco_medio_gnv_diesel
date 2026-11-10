"""
etl_gnv_diesel.py — Pipeline ETL unificado: Bronze -> Prata -> SQLite -> JSON.

Funções:
    bronze()           — passo01: filtra/padroniza CSVs do bronze para a prata.
    prata()            — passo02: carrega a prata no SQLite (gnv.db) + relatório.
    monta_descricao()  — passo03: monta a frase de descarte e retorna a string.
    monta_json()       — passo04: consulta o SQLite e grava etl/gnv_diesel.json.
    faxina()           — passo05: remove gnv.db* e CSVs da prata.

Cada função retorna um código de saída (0 = sucesso, != 0 = falha).
O main executa todas em sequência e aborta no primeiro erro.

Uso:
    python etl_gnv_diesel.py
"""

import json
import os
import re
import shutil
import sqlite3
import sys
import time
import warnings
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# ============================================================
# CONFIG
# ============================================================
BASE = Path(os.path.dirname(os.path.abspath(__file__)))

PASTA_BRONZE = Path(r"D:\ETL\nivel_01_bronze\diesel_gnv")
PASTA_PRATA = Path(r"D:\ETL\nivel_02_prata\diesel_gnv")
PASTA_LOG = BASE / "logs"
PASTA_LOG.mkdir(parents=True, exist_ok=True)

DB_PATH = BASE / "gnv.db"
ARQUIVO_JSON = BASE / "etl" / "gnv_diesel.json"

ARQUIVO_FRASE_TXT = BASE / "frase_descarte.txt"

timestamp_run = datetime.now().strftime("%Y%m%d_%H%M%S")
ARQUIVO_LOG = PASTA_LOG / f"etl_sqlite_gnv_{timestamp_run}.log"

ANOS = range(2022, 2027)
MESES = [f"{m:02d}" for m in range(1, 13)]

COLUNA_DATA = "Data da Coleta"

COLUNAS_NECESSARIAS = [
    "Regiao - Sigla",
    "Estado - Sigla",
    "Municipio",
    "CNPJ da Revenda",
    "Produto",
    "Data da Coleta",
    "Valor de Venda",
]

# nomes originais no CSV prata (o passo bronze não renomeia)
COLUNAS_ENTRADA = ["Estado - Sigla", "Produto", "Valor de Venda", "MES_ANO"]

RENOMEAR_ENTRADA = {
    "Estado - Sigla": "ESTADO",
    "Produto": "PRODUTO",
    "Valor de Venda": "VL_VENDA",
}

CAMPOS = ["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA"]

PRODUTOS_JSON = ["DIESEL S10", "DIESEL", "GNV"]

RESPONSAVEL = "Antonino Marques Jares"
NOME_PROGRAMA = "etl_gnv_diesel.py"
DESCRICAO_PADRAO = "Descrição não disponível (frase_descarte.txt não encontrado)."

SQL_POR_PRODUTO = """
    SELECT
        ESTADO,
        PRODUTO,
        MES_ANO,
        AVG(VL_VENDA) AS VL_VENDA_MEDIO
    FROM gnv
    WHERE PRODUTO = ?
    GROUP BY ESTADO, MES_ANO
    ORDER BY ESTADO, MES_ANO
"""

log_linhas = []


# ============================================================
# HELPERS GERAIS
# ============================================================
def log(mensagem: str = "", nivel: str = "INFO", arquivo: Path = ARQUIVO_LOG):
    linha = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{nivel}] {mensagem}"
    print(linha, flush=True)
    log_linhas.append(linha)
    try:
        with open(arquivo, "a", encoding="utf-8") as f:
            f.write(linha + "\n")
    except Exception:
        pass


def _num(n) -> str:
    return f"{n:,}".replace(",", ".")


def _pct(p: float) -> str:
    return f"{p:.2f}".replace(".", ",") + "%"


# ============================================================
# PASSO 01 — bronze()
# ============================================================
def _ler_csv_bronze(caminho: Path) -> pd.DataFrame:
    ultimo_erro = None
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            return pd.read_csv(caminho, sep=";", encoding=enc, low_memory=False)
        except UnicodeDecodeError as e:
            ultimo_erro = e
            continue
    raise RuntimeError(f"Não foi possível decodificar {caminho}: {ultimo_erro}")


def _converter_data(serie: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie
    convertido = pd.to_datetime(serie, format="%d/%m/%Y", errors="coerce")
    if convertido.isna().any():
        fallback = pd.to_datetime(serie, dayfirst=True, errors="coerce")
        convertido = convertido.fillna(fallback)
    return convertido


def bronze() -> int:
    """Passo 01 — Bronze -> Prata: filtra colunas e padroniza datas."""
    PASTA_PRATA.mkdir(parents=True, exist_ok=True)

    erros = 0
    processados = 0
    total_descartadas = 0

    for ano in ANOS:
        for mes in MESES:
            origem = PASTA_BRONZE / f"precos-diesel-gnv-{ano}-{mes}.csv"
            destino = PASTA_PRATA / f"precos-diesel-gnv-{ano}-{mes}.csv"

            if not origem.exists():
                log(f"⚠ Arquivo não encontrado: {origem}", "WARN")
                continue

            try:
                df = _ler_csv_bronze(origem)

                faltantes = [c for c in COLUNAS_NECESSARIAS if c not in df.columns]
                if faltantes:
                    log(f"⚠ Colunas faltantes em {origem.name}: {faltantes}", "WARN")
                    erros += 1
                    continue

                df = df[COLUNAS_NECESSARIAS].copy()
                bruto_data = df[COLUNA_DATA].copy()
                df[COLUNA_DATA] = _converter_data(df[COLUNA_DATA])

                mask_invalida = df[COLUNA_DATA].isna()
                n_invalidas = int(mask_invalida.sum())
                if n_invalidas:
                    total_descartadas += n_invalidas
                    log(f"⚠ {origem.name}: {n_invalidas} linha(s) com "
                        f"'{COLUNA_DATA}' não interpretável — descartada(s)", "WARN")
                    top = bruto_data.loc[mask_invalida].astype(str).value_counts().head(5)
                    for v, q in top.items():
                        log(f"     {q:>5}x  {v!r}", "WARN")

                df = df[~mask_invalida].copy()

                df["ANO"] = df[COLUNA_DATA].dt.year.astype("Int64")
                df["MES"] = df[COLUNA_DATA].dt.month.astype("Int64")
                df["MES_ANO"] = (
                    df["MES"].astype("string").str.zfill(2)
                    + "/"
                    + df["ANO"].astype("string")
                )

                df.to_csv(destino, index=False, encoding="utf-8")
                processados += 1
                log(f"✅ {destino.name} salvo ({len(df)} linhas)")

            except Exception as e:
                log(f"❌ Erro em {origem.name}: {e}", "ERROR")
                erros += 1

    log(f"Resumo bronze: {processados} arquivo(s) processado(s), "
        f"{erros} erro(s), {total_descartadas} linha(s) descartada(s) por data inválida.")
    return 1 if erros > 0 else 0


# ============================================================
# PASSO 02 — prata()
# ============================================================
def _to_str(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    return str(v).strip()


def _normalizar_valor(serie: pd.Series) -> pd.Series:
    s = serie.astype(str).str.strip()
    s = s.str.replace(r"[R$\s]", "", regex=True)
    tem_virgula = s.str.contains(",", na=False)
    s = s.where(
        ~tem_virgula,
        s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
    )
    s = s.str.replace(r"[^\d.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


def _classificar_motivo(valor_original) -> str:
    v = _to_str(valor_original)
    if v == "" or v.lower() in ("nan", "none", "null"):
        return "VAZIO"
    if v in ("-", "--", "---", "N/A", "n/a", "NA", "n.d.", "ND"):
        return "MARCADOR_SEM_DADO"
    if re.fullmatch(r"[R$\s]*0+[,.]?0*", v):
        return "ZERO"
    if not re.search(r"\d", v):
        return "SEM_DIGITO"
    return "FORMATO_INVALIDO"


def _imprimir_valores_nao_convertidos(arquivo_nome, originais, motivos):
    if len(originais) == 0:
        return
    log(f"  {arquivo_nome}: {len(originais)} valor(es) de VL_VENDA não convertido(s) -> NULL", "WARN")
    pares = defaultdict(int)
    for valor, motivo in zip(originais, motivos):
        pares[(_to_str(valor)[:60], motivo)] += 1
    for (valor, motivo), qtd in sorted(pares.items(), key=lambda x: -x[1]):
        v = valor if valor else "<vazio>"
        log(f"    {qtd:>6}x  motivo={motivo:<18}  valor={v!r}", "WARN")


def _validar_cobertura(arquivos):
    por_ano = defaultdict(set)
    for a in arquivos:
        m = re.search(r"-(\d{4})-(\d{2})\.csv$", a.name)
        if m:
            por_ano[m.group(1)].add(m.group(2))
    if not por_ano:
        log("Nenhum nome casou com o padrão YYYY-MM.", "WARN")
        return
    anos = sorted(por_ano)
    log(f"Cobertura: {anos[0]}-{min(por_ano[anos[0]])} até {anos[-1]}-{max(por_ano[anos[-1]])}")
    for i, ano in enumerate(anos):
        meses = por_ano[ano]
        esperado = set(f"{m:02d}" for m in range(1, 13))
        if i == 0:
            esperado = {m for m in esperado if m >= min(meses)}
        if i == len(anos) - 1:
            esperado = {m for m in esperado if m <= max(meses)}
        faltando = esperado - meses
        if faltando:
            log(f"  {ano}: meses faltando no meio do ano -> {sorted(faltando)}", "WARN")


def _preparar_banco(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS gnv")
    cur.execute("""
        CREATE TABLE gnv (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ESTADO TEXT,
            PRODUTO TEXT,
            MES_ANO TEXT,
            VL_VENDA FLOAT
        )
    """)
    conn.commit()
    log("Tabela gnv recriada.")


def _estatisticas_mes_ano(conn) -> dict:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gnv")
    total = cur.fetchone()[0] or 0
    cur.execute("""
        SELECT COUNT(*) FROM gnv
        WHERE MES_ANO IS NULL
           OR TRIM(MES_ANO) = ''
           OR LOWER(TRIM(MES_ANO)) = 'nan'
    """)
    nulos = cur.fetchone()[0] or 0
    pct = (100.0 * nulos / total) if total else 0.0
    return {"total": total, "nulos": nulos, "nao_nulos": total - nulos, "pct_nulos": pct}


def _produtos_com_mes_ano_nulo(conn) -> list:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            PRODUTO,
            COUNT(*) AS total_registros,
            SUM(CASE
                    WHEN MES_ANO IS NULL
                      OR TRIM(MES_ANO) = ''
                      OR LOWER(TRIM(MES_ANO)) = 'nan'
                    THEN 1 ELSE 0
                END) AS mes_ano_nulos,
            ROUND(
                100.0 * SUM(CASE
                                WHEN MES_ANO IS NULL
                                  OR TRIM(MES_ANO) = ''
                                  OR LOWER(TRIM(MES_ANO)) = 'nan'
                                THEN 1 ELSE 0
                            END) / COUNT(*),
                2
            ) AS pct_nulos_no_produto
        FROM gnv
        GROUP BY PRODUTO
        ORDER BY mes_ano_nulos DESC
    """)
    resultado = []
    for produto, total, nulos, pct in cur.fetchall():
        resultado.append({
            "produto": produto if produto is not None else "<NULL>",
            "total_registros": total or 0,
            "mes_ano_nulos": nulos or 0,
            "pct_nulos_no_produto": pct if pct is not None else 0.0,
        })
    return resultado


def _estatisticas_mes_ano_por_produto_estado(conn, top_n: int = 30) -> list:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            PRODUTO,
            ESTADO,
            COUNT(*) AS total,
            SUM(CASE
                    WHEN MES_ANO IS NULL
                      OR TRIM(MES_ANO) = ''
                      OR LOWER(TRIM(MES_ANO)) = 'nan'
                    THEN 1 ELSE 0
                END) AS nulos
        FROM gnv
        GROUP BY PRODUTO, ESTADO
        HAVING nulos > 0
        ORDER BY nulos DESC
        LIMIT ?
    """, (top_n,))
    resultado = []
    for produto, estado, total, nulos in cur.fetchall():
        nulos = nulos or 0
        total = total or 0
        pct = (100.0 * nulos / total) if total else 0.0
        resultado.append({
            "produto": produto if produto is not None else "<NULL>",
            "estado": estado if estado is not None else "<NULL>",
            "total": total,
            "nulos_mes_ano": nulos,
            "pct_nulos": pct,
        })
    return resultado


def prata() -> int:
    """Passo 02 — Prata -> SQLite (gnv.db) + relatório de qualidade."""
    stats = {
        "arquivos_lidos": 0,
        "linhas_lidas": 0,
        "linhas_inseridas": 0,
        "linhas_vazias_ignoradas": 0,
        "erros": 0,
    }
    nulos_campo_global = {c: 0 for c in CAMPOS}
    nulos_por_arquivo = {}
    nulos_motivo_vl_venda = defaultdict(int)
    nulos_valor_original_vl_venda = defaultdict(int)
    mes_ano_stats = {"total": 0, "nulos": 0, "nao_nulos": 0, "pct_nulos": 0.0}
    produtos_mes_ano_nulo = []
    mes_ano_por_produto_estado = []

    conn = None
    cursor = None
    try:
        arquivos = sorted(PASTA_PRATA.glob("precos-diesel-gnv-*.csv"))
        log(f"{len(arquivos)} arquivo(s) encontrado(s) em {PASTA_PRATA}")
        if not arquivos:
            log("Pasta sem arquivos compatíveis. Abortando.", "ERROR")
            return 1

        _validar_cobertura(arquivos)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        log(f"Conectado ao banco: {DB_PATH}")

        _preparar_banco(conn)

        for arquivo in arquivos:
            try:
                df = pd.read_csv(arquivo, sep=",", encoding="utf-8", low_memory=False)

                antes = len(df)
                df = df.dropna(how="all")
                vazias = antes - len(df)
                if vazias:
                    stats["linhas_vazias_ignoradas"] += vazias
                    log(f"{arquivo.name}: {vazias} linha(s) totalmente vazia(s) ignorada(s)")

                faltantes = [c for c in COLUNAS_ENTRADA if c not in df.columns]
                if faltantes:
                    log(f"{arquivo.name}: colunas faltantes {faltantes}. "
                        f"Colunas no CSV: {df.columns.tolist()}", "ERROR")
                    stats["erros"] += 1
                    continue

                total_arquivo = len(df)
                stats["linhas_lidas"] += total_arquivo

                df = df[COLUNAS_ENTRADA].rename(columns=RENOMEAR_ENTRADA)

                campos_nulos_arq = {}
                for c in CAMPOS:
                    if c == "VL_VENDA":
                        continue
                    serie_str = df[c].astype(str).str.strip()
                    nulos = int((df[c].isna()
                                 | (serie_str == "")
                                 | (serie_str.str.lower() == "nan")).sum())
                    campos_nulos_arq[c] = nulos
                    nulos_campo_global[c] += nulos

                df["VL_VENDA_NUM"] = _normalizar_valor(df["VL_VENDA"])
                mask_nulo_vl = df["VL_VENDA_NUM"].isna()
                qtd_nulos_vl = int(mask_nulo_vl.sum())
                campos_nulos_arq["VL_VENDA"] = qtd_nulos_vl
                nulos_campo_global["VL_VENDA"] += qtd_nulos_vl

                originais_nulos = df.loc[mask_nulo_vl, "VL_VENDA"]
                motivos_nulos = [_classificar_motivo(v) for v in originais_nulos]

                for valor, motivo in zip(originais_nulos, motivos_nulos):
                    nulos_motivo_vl_venda[motivo] += 1
                    nulos_valor_original_vl_venda[_to_str(valor)[:60]] += 1

                _imprimir_valores_nao_convertidos(arquivo.name, originais_nulos, motivos_nulos)

                nulos_por_arquivo[arquivo.name] = {
                    "total": total_arquivo,
                    "campos": campos_nulos_arq,
                }

                saida = df[["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA_NUM"]].rename(
                    columns={"VL_VENDA_NUM": "VL_VENDA"}
                )
                saida.to_sql("gnv", conn, if_exists="append", index=False)

                stats["arquivos_lidos"] += 1
                stats["linhas_inseridas"] += len(saida)

                log(f"{arquivo.name}: {len(saida)} inseridos "
                    f"(nulos: ESTADO={campos_nulos_arq['ESTADO']}, "
                    f"PRODUTO={campos_nulos_arq['PRODUTO']}, "
                    f"MES_ANO={campos_nulos_arq['MES_ANO']}, "
                    f"VL_VENDA={campos_nulos_arq['VL_VENDA']})")

            except Exception as e:
                log(f"Erro ao processar {arquivo.name}: {e}", "ERROR")
                stats["erros"] += 1

        cursor.execute("SELECT COUNT(*) FROM gnv")
        total = cursor.fetchone()[0]
        log(f"Total de registros na tabela gnv: {total}")

        est = _estatisticas_mes_ano(conn)
        mes_ano_stats.update(est)
        log("MES_ANO nulos: "
            f"{_num(est['nulos'])} de {_num(est['total'])} "
            f"({est['pct_nulos']:.2f}%) — não nulos: {_num(est['nao_nulos'])}")

        produtos_mes_ano_nulo = _produtos_com_mes_ano_nulo(conn)

        log("-" * 70)
        log("PRODUTOS COM MES_ANO NULO (consulta direta em gnv):")
        if produtos_mes_ano_nulo:
            log(f"  {'PRODUTO':<14} {'TOTAL':>12} {'NULOS':>12} {'% do produto':>14}")
            for d in produtos_mes_ano_nulo:
                log(f"  {d['produto']:<14} {_num(d['total_registros']):>12} "
                    f"{_num(d['mes_ano_nulos']):>12} {d['pct_nulos_no_produto']:>13.2f}%")
            com_nulo = [d for d in produtos_mes_ano_nulo if d["mes_ano_nulos"] > 0]
            if com_nulo:
                log("  -> Produtos afetados: " + ", ".join(d["produto"] for d in com_nulo))
            else:
                log("  -> Nenhum produto com MES_ANO nulo.")
        else:
            log("  Tabela gnv vazia — nada a reportar.")

        mes_ano_por_produto_estado = _estatisticas_mes_ano_por_produto_estado(conn, top_n=30)
        if mes_ano_por_produto_estado:
            log("-" * 70)
            log("TOP 30 (PRODUTO, ESTADO) com mais MES_ANO nulo:")
            log(f"  {'PRODUTO':<14} {'ESTADO':<8} {'NULOS':>10} {'TOTAL':>10} {'%':>8}")
            for d in mes_ano_por_produto_estado:
                log(f"  {d['produto']:<14} {d['estado']:<8} "
                    f"{_num(d['nulos_mes_ano']):>10} {_num(d['total']):>10} "
                    f"{d['pct_nulos']:>7.2f}%")

        if stats["linhas_inseridas"] == 0:
            log("Nenhuma linha inserida. Considerando falha.", "ERROR")
            return 1
        if stats["erros"] > 0:
            log(f"{stats['erros']} erro(s) de processamento. Considerando falha.", "ERROR")
            return 1
        return 0

    except Exception as e:
        log(f"ERRO CRÍTICO: {e}", "ERROR")
        return 1

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                log(f"Erro ao fechar cursor: {e}", "ERROR")
        if conn is not None:
            try:
                conn.close()
                log("Conexão com o banco encerrada.")
            except Exception as e:
                log(f"Erro ao fechar conexão: {e}", "ERROR")

        log("=" * 70)
        log("RESUMO PRATA")
        log(f"  Arquivos lidos:            {stats['arquivos_lidos']}")
        log(f"  Linhas lidas:              {stats['linhas_lidas']}")
        log(f"  Linhas inseridas:          {stats['linhas_inseridas']}")
        log(f"  Linhas vazias ignoradas:   {stats['linhas_vazias_ignoradas']}")
        log("  Nulos por campo:")
        for c in CAMPOS:
            log(f"    {c:<10} {nulos_campo_global[c]}")
        log(f"  Erros:                     {stats['erros']}")

        if mes_ano_stats["total"]:
            log("  MES_ANO nulos:             "
                f"{_num(mes_ano_stats['nulos'])} / {_num(mes_ano_stats['total'])} "
                f"({mes_ano_stats['pct_nulos']:.2f}%)")

        if nulos_valor_original_vl_venda:
            log("-" * 70)
            log("TOP 20 VALORES ORIGINAIS QUE VIRARAM NULL EM VL_VENDA (global)")
            for valor, qtd in sorted(nulos_valor_original_vl_venda.items(), key=lambda x: -x[1])[:20]:
                v = valor if valor else "<vazio>"
                log(f"  {qtd:>6}x  {v!r}")

        log("=" * 70)
        log("FIM PRATA")

        


# ============================================================
# PASSO 03 — monta_descricao()
# ============================================================
def _contar_nulos_bronze(df: pd.DataFrame) -> int:
    if COLUNA_DATA not in df.columns:
        raise KeyError(f"Coluna {COLUNA_DATA!r} ausente. Colunas: {df.columns.tolist()}")
    serie = df[COLUNA_DATA]
    if pd.api.types.is_datetime64_any_dtype(serie):
        convertido = serie
    else:
        convertido = pd.to_datetime(serie, format="%d/%m/%Y", errors="coerce")
        if convertido.isna().any():
            fallback = pd.to_datetime(serie, dayfirst=True, errors="coerce")
            convertido = convertido.fillna(fallback)
    return int(convertido.isna().sum())


def monta_descricao() -> str:
    """Passo 03 — Monta a frase dinâmica de descarte e retorna a string."""
    if not PASTA_BRONZE.exists():
        msg = f"[ERRO] Pasta não encontrada: {PASTA_BRONZE}"
        print(msg)
        log(msg, "ERROR")
        return DESCRICAO_PADRAO

    arquivos = sorted(PASTA_BRONZE.glob("precos-diesel-gnv-*.csv"))
    if not arquivos:
        msg = f"[ERRO] Nenhum CSV encontrado em: {PASTA_BRONZE}"
        print(msg)
        log(msg, "ERROR")
        return DESCRICAO_PADRAO

    total_lidos = 0
    total_nulos = 0

    for f in arquivos:
        try:
            df = pd.read_csv(f, sep=";", encoding="latin1", low_memory=False)
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(f, sep=";", encoding="cp1252", low_memory=False)
            except Exception as e:
                print(f"[AVISO] Falha ao ler {f.name}: {e}")
                continue
        except Exception as e:
            print(f"[AVISO] Falha ao ler {f.name}: {e}")
            continue

        if COLUNA_DATA not in df.columns:
            print(f"[AVISO] {f.name} não tem a coluna {COLUNA_DATA!r}.")
            continue

        total_lidos += len(df)
        total_nulos += _contar_nulos_bronze(df)

    if total_lidos == 0:
        msg = "[ERRO] Nenhum registro lido. Não há frase para montar."
        print(msg)
        log(msg, "ERROR")
        return DESCRICAO_PADRAO

    pct = 100 * total_nulos / total_lidos
    frase = (f"De {_num(total_lidos)} registros lidos, "
             f"{_num(total_nulos)} ({_pct(pct)}) "
             f"foram descartados por conter valor nulo na coluna '{COLUNA_DATA}'.")

    print()
    print("=" * 78)
    print("FRASE DINÂMICA DE DESCARTE")
    print("=" * 78)
    print(frase)
    print("=" * 78)
    print()

    try:
        ARQUIVO_FRASE_TXT.write_text(frase + "\n", encoding="utf-8")
        print(f"📄 Frase salva em: {ARQUIVO_FRASE_TXT}")
    except Exception as e:
        print(f"[AVISO] Não foi possível salvar {ARQUIVO_FRASE_TXT}: {e}")

    return frase


def monta_descricao() -> str:
    """Passo 03 — Monta a frase dinâmica de descarte e retorna a string."""
    if not PASTA_BRONZE.exists():
        msg = f"[ERRO] Pasta não encontrada: {PASTA_BRONZE}"
        print(msg)
        log(msg, "ERROR")
        return DESCRICAO_PADRAO

    arquivos = sorted(PASTA_BRONZE.glob("precos-diesel-gnv-*.csv"))
    if not arquivos:
        msg = f"[ERRO] Nenhum CSV encontrado em: {PASTA_BRONZE}"
        print(msg)
        log(msg, "ERROR")
        return DESCRICAO_PADRAO

    total_lidos = 0
    total_nulos = 0

    for f in arquivos:
        try:
            df = pd.read_csv(f, sep=";", encoding="latin1", low_memory=False)
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(f, sep=";", encoding="cp1252", low_memory=False)
            except Exception as e:
                print(f"[AVISO] Falha ao ler {f.name}: {e}")
                continue
        except Exception as e:
            print(f"[AVISO] Falha ao ler {f.name}: {e}")
            continue

        if COLUNA_DATA not in df.columns:
            print(f"[AVISO] {f.name} não tem a coluna {COLUNA_DATA!r}.")
            continue

        total_lidos += len(df)
        total_nulos += _contar_nulos_bronze(df)

    if total_lidos == 0:
        msg = "[ERRO] Nenhum registro lido. Não há frase para montar."
        print(msg)
        log(msg, "ERROR")
        return DESCRICAO_PADRAO

    pct = 100 * total_nulos / total_lidos
    frase = (f"De {_num(total_lidos)} registros lidos, "
             f"{_num(total_nulos)} ({_pct(pct)}) "
             f"foram descartados por conter valor nulo na coluna '{COLUNA_DATA}'.")

    print()
    print("=" * 78)
    print("FRASE DINÂMICA DE DESCARTE")
    print("=" * 78)
    print(frase)
    print("=" * 78)
    print()

    try:
        ARQUIVO_FRASE_TXT.write_text(frase + "\n", encoding="utf-8")
        print(f"📄 Frase salva em: {ARQUIVO_FRASE_TXT}")
    except Exception as e:
        print(f"[AVISO] Não foi possível salvar {ARQUIVO_FRASE_TXT}: {e}")

    payload = {
        "frase": frase,
        "total_lidos": total_lidos,
        "total_nulos": total_nulos,
        "percentual": _pct(pct),
        "coluna": COLUNA_DATA,
    }

    return frase


# ============================================================
# PASSO 04 — monta_json()
# ============================================================
def _ler_frase_descarte() -> str:
    if not ARQUIVO_FRASE_TXT.exists():
        log(f"[AVISO] {ARQUIVO_FRASE_TXT.name} não encontrado. Usando descrição padrão.", "WARN")
        return DESCRICAO_PADRAO
    try:
        frase = ARQUIVO_FRASE_TXT.read_text(encoding="utf-8").strip()
    except UnicodeDecodeError:
        try:
            frase = ARQUIVO_FRASE_TXT.read_text(encoding="latin1").strip()
        except Exception as e:
            log(f"[AVISO] Falha ao ler {ARQUIVO_FRASE_TXT.name}: {e}. Usando descrição padrão.", "WARN")
            return DESCRICAO_PADRAO
    except Exception as e:
        log(f"[AVISO] Falha ao ler {ARQUIVO_FRASE_TXT.name}: {e}. Usando descrição padrão.", "WARN")
        return DESCRICAO_PADRAO
    if not frase:
        log(f"[AVISO] {ARQUIVO_FRASE_TXT.name} está vazio. Usando descrição padrão.", "WARN")
        return DESCRICAO_PADRAO
    return frase


def _validar_schema(conn):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(gnv)").fetchall()]
    obrigatorias = ["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA"]
    faltantes = [c for c in obrigatorias if c not in cols]
    if faltantes:
        raise RuntimeError(f"Colunas faltando em gnv: {faltantes}. Colunas atuais: {cols}.")
    return cols


def _rodar_queries(conn):
    frames = []
    for produto in PRODUTOS_JSON:
        df = pd.read_sql_query(SQL_POR_PRODUTO, conn, params=(produto,))
        print(f"  {produto:<12} -> {len(df):>6} linha(s)")
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df_final = pd.concat(frames, ignore_index=True)
    df_final["VL_VENDA_MEDIO"] = df_final["VL_VENDA_MEDIO"].round(4)
    df_final = df_final.sort_values(["ESTADO", "PRODUTO", "MES_ANO"]).reset_index(drop=True)
    return df_final


def monta_json() -> int:
    """Passo 04 — Consulta o SQLite e grava etl/gnv_diesel.json."""
    if not DB_PATH.exists():
        log(f"[ERRO] Banco não encontrado: {DB_PATH}. Rode a etapa prata antes.", "ERROR")
        return 1

    print("=" * 78)
    print(f"Banco: {DB_PATH}")
    print(f"Produtos: {PRODUTOS_JSON}")
    print(f"Frase de descarte: {ARQUIVO_FRASE_TXT}")
    print("=" * 78)

    conn = sqlite3.connect(DB_PATH)
    try:
        cols = _validar_schema(conn)
        print(f"Colunas em gnv: {cols}\n")
        print("Rodando as 3 queries (por ESTADO):")
        df = _rodar_queries(conn)
    finally:
        conn.close()

    if df.empty:
        log("\n[AVISO] Nenhum registro retornado. Verifique: SELECT DISTINCT PRODUTO FROM gnv;", "WARN")
        return 1

    print(f"\nTotal consolidado: {_num(len(df))} linha(s)")
    print("\nAmostra:")
    print(df.head(10).to_string(index=False))

    ARQUIVO_JSON.parent.mkdir(parents=True, exist_ok=True)
    descricao = _ler_frase_descarte()

    payload = {
        "metadados": {
            "titulo": "Preços médios de venda de DIESEL S10, DIESEL e GNV por MES e ESTADO",
            "data_de_atualizacao": datetime.now().strftime("%d/%m/%Y"),
            "responsavel": RESPONSAVEL,
            "nome_programa": NOME_PROGRAMA,
            "produtos": "DIESEL S10, DIESEL e GNV",
            "descricao": descricao,
        },
        "dados": df.to_dict(orient="records"),
    }

    try:
        with open(ARQUIVO_JSON, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except UnicodeEncodeError:
        with open(ARQUIVO_JSON, "w", encoding="latin1") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    tamanho = ARQUIVO_JSON.stat().st_size / 1024
    print(f"\nJSON gerado: {ARQUIVO_JSON} ({tamanho:.1f} KB)")
    print(f"Total de registros: {_num(len(df))}")
    print(f"Descrição (primeiros 120 chars): {descricao[:120]!r}")
    return 0


# ============================================================
# PASSO 05 — faxina()
# ============================================================
def _remover_arquivo(caminho: Path) -> bool:
    try:
        os.remove(caminho)
        print(f"🗑  Removido: {caminho}")
        return True
    except FileNotFoundError:
        print(f"ℹ  Não existe (ok): {caminho}")
        return True
    except PermissionError:
        print(f"❌ Sem permissão (arquivo em uso?): {caminho}")
        print("   Feche o programa que está usando este arquivo e rode de novo.")
        return False
    except Exception as e:
        print(f"❌ Erro ao remover {caminho}: {e}")
        return False


def faxina() -> int:
    """Passo 05 — Remove gnv.db*, CSVs da prata e frase_descarte.*"""
    print("=" * 60)
    print("LIMPEZA DE ARTEFATOS — ETL GNV/DIESEL")
    print("=" * 60)

    # --- banco ---
    print("\n--- Removendo banco SQLite (e auxiliares WAL/SHM) ---")
    print(f"   Projeto: {BASE}")
    ok_banco = True
    encontrou_algo = False
    for sufixo in ("", "-wal", "-shm"):
        caminho = DB_PATH if sufixo == "" else DB_PATH.with_name(DB_PATH.name + sufixo)
        if caminho.exists():
            encontrou_algo = True
            ok_banco = _remover_arquivo(caminho) and ok_banco
    if not encontrou_algo:
        print(f"ℹ  Nenhum arquivo gnv.db* encontrado em {BASE} (ok)")

    # --- frase de descarte ---
    print("\n--- Removendo frase_descarte.* ---")
    ok_frase = True
    frase_encontrada = False
    for nome in ("frase_descarte.txt", "frase_descarte.json"):
        caminho = BASE / nome
        if caminho.exists():
            frase_encontrada = True
            ok_frase = _remover_arquivo(caminho) and ok_frase
    if not frase_encontrada:
        print(f"ℹ  Nenhum arquivo frase_descarte.* encontrado em {BASE} (ok)")

    # --- csvs prata ---
    print("\n--- Removendo CSVs da pasta prata ---")
    ok_csvs = True
    if not PASTA_PRATA.exists():
        print(f"ℹ  Pasta não existe (ok): {PASTA_PRATA}")
    else:
        csvs = sorted(PASTA_PRATA.glob("*.csv"))
        if not csvs:
            print(f"ℹ  Nenhum CSV encontrado em: {PASTA_PRATA}")
        else:
            print(f"   {len(csvs)} CSV(s) encontrado(s).")
            for arq in csvs:
                if not _remover_arquivo(arq):
                    ok_csvs = False

    print("\n" + "=" * 60)
    if ok_banco and ok_frase and ok_csvs:
        print("✅ Limpeza concluída com sucesso.")
        return 0
    print("⚠  Limpeza concluída com falhas (veja acima).")
    return 1


# ============================================================
# MAIN — pipeline unificado
# ============================================================
PASSOS = [
    ("bronze",          "Bronze -> Prata: filtra e padroniza CSV",                  bronze),
    ("prata",           "Prata -> SQLite (gnv.db) + relatório de qualidade",         prata),
    ("monta_descricao", "Monta descrição dinâmica e salva frase_descarte.*",        monta_descricao),
    ("monta_json",      "SQLite -> JSON de preços médios por estado",                monta_json),
    ("faxina",          "Limpeza de artefatos gerados pelo pipeline",                faxina),
]


def main() -> int:
    print(f"\n{'#' * 78}")
    print("# PIPELINE ETL — Preços médios GNV/Diesel (unificado)")
    print(f"# Base: {BASE}")
    print(f"# Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#' * 78}\n")

    total_inicio = time.time()

    for nome, descricao, funcao in PASSOS:
        print("=" * 78)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] >>> {nome} — {descricao}")
        print("=" * 78)

        inicio = time.time()
        try:
            if nome == "monta_descricao":
                # retorna string, mas o pipeline só precisa do código de saída
                frase = funcao()
                code = 0 if frase and frase != DESCRICAO_PADRAO else 1
            else:
                code = funcao()
        except KeyboardInterrupt:
            print(f"\n[INTERROMPIDO] {nome} cancelado pelo usuário (Ctrl+C).")
            return 130
        except Exception as e:
            print(f"\n[ERRO] Falha ao executar {nome}: {e}")
            return 1

        duracao = time.time() - inicio
        print(f"\n[{nome}] concluído em {duracao:.1f}s — código de saída: {code}")

        if code != 0:
            print(f"\n[ABORTADO] {nome} retornou código {code}. Pipeline interrompido.")
            return code

    total = time.time() - total_inicio
    print(f"\n{'#' * 78}")
    print(f"# ✅ Pipeline concluído com sucesso em {total:.1f}s")
    print(f"# Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#' * 78}")
    print(f"\n📄 JSON gerado: {ARQUIVO_JSON}")
    return 0


if __name__ == "__main__":
    sys.exit(main())