"""
etl_preco_medio_gnv_diesel.py — Pipeline ETL unificado (Bronze -> Prata -> SQLite -> JSON).

Etapas:
    1) passo01: Bronze -> Prata (CSVs resumidos)
    2) passo02: Prata -> SQLite (tabela gnv) + relatório de qualidade
    3) passo03: SQLite -> JSON consolidado (DIESEL S10, DIESEL, GNV)

Saídas:
    - CSVs Prata:    D:\\ETL\\nivel_02_prata\\diesel_gnv\\precos-diesel-gnv-resumo-{ano}-{mes}.csv
    - Banco:         <projeto>\\gnv.db (tabela gnv)
    - Log:           <projeto>\\logs\\etl_sqlite_gnv_<timestamp>.log
    - Relatório:     <projeto>\\relatorio_de_qualidade_de_dados.txt
    - JSON final:    <projeto>\\etl\\preco_medio_gnv_diesel_por_estado.json

Uso:
    python etl_preco_medio_gnv_diesel.py
"""

import json
import re
import sqlite3
import sys
import warnings
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

# ============================================================
# CONFIG
# ============================================================
BASE = Path(r"C:\Users\Nino\ProjetosPy\preco_medio_gnv_diesel")
PASTA_LOG = BASE / "logs"
PASTA_LOG.mkdir(parents=True, exist_ok=True)
PASTA_ETL = BASE / "etl"
PASTA_ETL.mkdir(parents=True, exist_ok=True)

PASTA_BRONZE = Path(r"D:\ETL\nivel_01_bronze\diesel_gnv")
PASTA_PRATA = Path(r"D:\ETL\nivel_02_prata\diesel_gnv")
DB_PATH = BASE / "gnv.db"

ARQUIVO_JSON = PASTA_ETL / "preco_medio_gnv_diesel_por_estado.json"
ARQUIVO_RELATORIO = BASE / "relatorio_de_qualidade_de_dados.txt"

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
ARQUIVO_LOG = PASTA_LOG / f"etl_sqlite_gnv_{TIMESTAMP}.log"

CAMPOS = ["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA"]
PRODUTOS = ["DIESEL S10", "DIESEL", "GNV"]

RESPONSAVEL = "Antonino Marques Jares"
NOME_PROGRAMA = "etl_preco_medio_gnv_diesel.py"

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

log_linhas = []


def log(mensagem: str, nivel: str = "INFO"):
    linha = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{nivel}] {mensagem}"
    print(linha, flush=True)
    log_linhas.append(linha)
    with open(ARQUIVO_LOG, "a", encoding="utf-8") as f:
        f.write(linha + "\n")


# ============================================================
# HELPERS
# ============================================================
def to_str(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    return str(v).strip()


def normalizar_valor(serie: pd.Series) -> pd.Series:
    """Converte 'R$ 5,89' / '5.89' / '5,89' / ' 5,89 ' em float."""
    s = serie.astype(str).str.strip()
    s = s.str.replace(r"[R$\s]", "", regex=True)
    tem_virgula = s.str.contains(",", na=False)
    s = s.where(
        ~tem_virgula,
        s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
    )
    s = s.str.replace(r"[^\d.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


def classificar_motivo(valor_original) -> str:
    v = to_str(valor_original)
    if v == "" or v.lower() in ("nan", "none", "null"):
        return "VAZIO"
    if v in ("-", "--", "---", "N/A", "n/a", "NA", "n.d.", "ND"):
        return "MARCADOR_SEM_DADO"
    if re.fullmatch(r"[R$\s]*0+[,.]?0*", v):
        return "ZERO"
    if not re.search(r"\d", v):
        return "SEM_DIGITO"
    return "FORMATO_INVALIDO"


def validar_cobertura(arquivos):
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


# ============================================================
# ETAPA 1 — BRONZE -> PRATA
# ============================================================
def etapa1_bronze_para_prata():
    log("=" * 70)
    log("ETAPA 1 — Bronze -> Prata")
    log("=" * 70)

    PASTA_PRATA.mkdir(parents=True, exist_ok=True)

    for ano in range(2022, 2027):
        for mes in ["01", "02", "03", "04", "05", "06",
                    "07", "08", "09", "10", "11", "12"]:
            origem = PASTA_BRONZE / f"precos-diesel-gnv-{ano}-{mes}.csv"
            destino = PASTA_PRATA / f"precos-diesel-gnv-resumo-{ano}-{mes}.csv"

            if not origem.exists():
                log(f"Arquivo não encontrado: {origem}", "WARN")
                continue

            try:
                df = None
                for enc in ("utf-8", "latin1", "cp1252"):
                    try:
                        df = pd.read_csv(origem, sep=";", encoding=enc, low_memory=False)
                        break
                    except UnicodeDecodeError:
                        continue
                if df is None:
                    log(f"Falha de encoding em {origem.name}", "ERROR")
                    continue

                colunas_necessarias = [
                    "Regiao - Sigla", "Estado - Sigla", "Municipio",
                    "CNPJ da Revenda", "Produto", "Data da Coleta", "Valor de Venda",
                ]
                faltantes = [c for c in colunas_necessarias if c not in df.columns]
                if faltantes:
                    log(f"Colunas faltantes em {origem.name}: {faltantes}", "WARN")
                    continue

                df = df[colunas_necessarias].rename(columns={
                    "Estado - Sigla": "ESTADO",
                    "Municipio": "MUNICIPIO",
                    "CNPJ da Revenda": "CNPJ",
                    "Produto": "PRODUTO",
                    "Data da Coleta": "DATA",
                    "Valor de Venda": "VL_VENDA",
                })

                df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")
                df["ANO"] = df["DATA"].dt.year.astype("Int64")
                df["MES"] = df["DATA"].dt.month.astype("Int64")
                df["MES_ANO"] = (
                    df["MES"].astype(str).str.zfill(2) + "/" + df["ANO"].astype(str)
                )

                df.to_csv(destino, index=False, encoding="utf-8")
                log(f"Salvo: {destino.name} ({len(df)} linhas)")

            except Exception as e:
                log(f"Erro em {origem.name}: {e}", "ERROR")

    log("ETAPA 1 concluída.")


# ============================================================
# ETAPA 2 — PRATA -> SQLITE (+ RELATÓRIO DE QUALIDADE)
# ============================================================
def etapa2_prata_para_sqlite():
    log("=" * 70)
    log("ETAPA 2 — Prata -> SQLite")
    log("=" * 70)

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

    arquivos = sorted(
        a for a in PASTA_PRATA.glob("precos-diesel-gnv-*.csv")
        if "resumo" not in a.name.lower()
    )
    log(f"{len(arquivos)} arquivo(s) mensal(is) encontrado(s) em {PASTA_PRATA}")

    if not arquivos:
        log("Pasta sem arquivos compatíveis. Abortando.", "ERROR")
        raise SystemExit(1)

    validar_cobertura(arquivos)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("DROP TABLE IF EXISTS gnv")
        cursor.execute("""
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

        for arquivo in arquivos:
            try:
                df = pd.read_csv(arquivo, sep=",", encoding="latin1", low_memory=False)

                antes = len(df)
                df = df.dropna(how="all")
                vazias = antes - len(df)
                if vazias:
                    stats["linhas_vazias_ignoradas"] += vazias
                    log(f"{arquivo.name}: {vazias} linha(s) vazia(s) ignorada(s)")

                faltantes = [c for c in CAMPOS if c not in df.columns]
                if faltantes:
                    log(f"{arquivo.name}: colunas faltantes {faltantes}", "ERROR")
                    stats["erros"] += 1
                    continue

                total_arquivo = len(df)
                stats["linhas_lidas"] += total_arquivo

                df = df[CAMPOS].copy()

                campos_nulos_arq = {}
                for c in CAMPOS:
                    if c == "VL_VENDA":
                        continue
                    serie_str = df[c].astype(str).str.strip()
                    nulos = int(
                        (df[c].isna()
                         | (serie_str == "")
                         | (serie_str.str.lower() == "nan")).sum()
                    )
                    campos_nulos_arq[c] = nulos
                    nulos_campo_global[c] += nulos

                df["VL_VENDA_NUM"] = normalizar_valor(df["VL_VENDA"])
                mask_nulo_vl = df["VL_VENDA_NUM"].isna()
                qtd_nulos_vl = int(mask_nulo_vl.sum())
                campos_nulos_arq["VL_VENDA"] = qtd_nulos_vl
                nulos_campo_global["VL_VENDA"] += qtd_nulos_vl

                if qtd_nulos_vl > 0:
                    for v in df.loc[mask_nulo_vl, "VL_VENDA"]:
                        motivo = classificar_motivo(v)
                        nulos_motivo_vl_venda[motivo] += 1
                        nulos_valor_original_vl_venda[to_str(v)[:60]] += 1

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

                log(
                    f"{arquivo.name}: {len(saida)} inseridos "
                    f"(nulos: ESTADO={campos_nulos_arq['ESTADO']}, "
                    f"PRODUTO={campos_nulos_arq['PRODUTO']}, "
                    f"MES_ANO={campos_nulos_arq['MES_ANO']}, "
                    f"VL_VENDA={campos_nulos_arq['VL_VENDA']})"
                )
            except Exception as e:
                log(f"Erro ao processar {arquivo.name}: {e}", "ERROR")
                stats["erros"] += 1

        cursor.execute("SELECT COUNT(*) FROM gnv")
        total = cursor.fetchone()[0]
        log(f"Total de registros na tabela gnv: {total}")

    finally:
        try:
            cursor.close()
        except Exception as e:
            log(f"Erro ao fechar cursor: {e}", "ERROR")
        try:
            conn.close()
            log("Conexão com o banco encerrada.")
        except Exception as e:
            log(f"Erro ao fechar conexão: {e}", "ERROR")

    # -------- RESUMO NO LOG --------
    log("=" * 70)
    log("RESUMO FINAL — ETAPA 2")
    log(f"  Arquivos lidos:            {stats['arquivos_lidos']}")
    log(f"  Linhas lidas:              {stats['linhas_lidas']}")
    log(f"  Linhas inseridas:          {stats['linhas_inseridas']}")
    log(f"  Linhas vazias ignoradas:   {stats['linhas_vazias_ignoradas']}")
    log("  Nulos por campo:")
    for c in CAMPOS:
        log(f"    {c:<10} {nulos_campo_global[c]}")
    log(f"  Erros:                     {stats['erros']}")
    log("=" * 70)

    # -------- RELATÓRIO DE QUALIDADE --------
    def _num(n):
        return f"{n:,}".replace(",", ".")

    with open(ARQUIVO_RELATORIO, "w", encoding="utf-8") as f:
        def w(s=""):
            f.write(str(s) + "\n")

        w("=" * 78)
        w("RELATÓRIO DE QUALIDADE DE DADOS — ETL GNV")
        w(f"Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        w(f"Pasta Prata: {PASTA_PRATA}")
        w(f"Arquivos processados: {stats['arquivos_lidos']}")
        w("=" * 78)
        w()

        w("─" * 78)
        w("1) RESUMO GERAL")
        w("─" * 78)
        w(f"  Linhas lidas:                    {_num(stats['linhas_lidas'])}")
        w(f"  Linhas inseridas na tabela gnv:  {_num(stats['linhas_inseridas'])}")
        w(f"  Linhas totalmente vazias:        {_num(stats['linhas_vazias_ignoradas'])} (ignoradas)")
        w(f"  Erros de processamento:          {_num(stats['erros'])}")
        w()

        w("─" * 78)
        w("2) TOTAIS DE NULOS POR CAMPO")
        w("─" * 78)
        w(f"  {'CAMPO':<12} {'QTD NULOS':>12} {'% sobre linhas inseridas':>28}")
        base = stats["linhas_inseridas"] or 1
        for c in CAMPOS:
            qtd = nulos_campo_global[c]
            pct = 100 * qtd / base
            w(f"  {c:<12} {_num(qtd):>12} {pct:>27.2f}%")
        w()

        w("─" * 78)
        w("3) NULOS EM VL_VENDA — POR MOTIVO")
        w("─" * 78)
        if nulos_motivo_vl_venda:
            w(f"  {'MOTIVO':<22} {'QTD':>12} {'% dos nulos VL_VENDA':>22}")
            total_vl = nulos_campo_global["VL_VENDA"] or 1
            for motivo, qtd in sorted(nulos_motivo_vl_venda.items(), key=lambda x: -x[1]):
                pct = 100 * qtd / total_vl
                w(f"  {motivo:<22} {_num(qtd):>12} {pct:>21.2f}%")
        else:
            w("  Nenhum nulo em VL_VENDA.")
        w()

        w("─" * 78)
        w("4) TOP 30 VALORES ORIGINAIS NULOS EM VL_VENDA")
        w("─" * 78)
        if nulos_valor_original_vl_venda:
            w(f"  {'VALOR ORIGINAL':<40} {'QTD':>12}")
            for valor, qtd in sorted(nulos_valor_original_vl_venda.items(), key=lambda x: -x[1])[:30]:
                v = valor if valor else "<vazio>"
                w(f"  {v[:38]:<40} {_num(qtd):>12}")
        else:
            w("  Nenhum nulo em VL_VENDA.")
        w()

        w("─" * 78)
        w("5) NULOS POR ARQUIVO (todos os campos)")
        w("─" * 78)
        w(f"  {'ARQUIVO':<36} {'TOTAL':>8} {'ESTADO':>8} {'PRODUTO':>8} {'MES_ANO':>8} {'VL_VENDA':>9}")
        for arq in sorted(nulos_por_arquivo):
            d = nulos_por_arquivo[arq]
            c = d["campos"]
            w(f"  {arq:<36} {_num(d['total']):>8} "
              f"{_num(c['ESTADO']):>8} {_num(c['PRODUTO']):>8} "
              f"{_num(c['MES_ANO']):>8} {_num(c['VL_VENDA']):>9}")
        w()

        w("─" * 78)
        w("6) ARQUIVOS COM QUALQUER CAMPO NULO")
        w("─" * 78)
        algum = False
        for arq in sorted(nulos_por_arquivo):
            d = nulos_por_arquivo[arq]
            c = d["campos"]
            if any(c[k] > 0 for k in CAMPOS):
                algum = True
                detalhes = ", ".join(f"{k}={_num(c[k])}" for k in CAMPOS if c[k] > 0)
                w(f"  {arq}  ->  {detalhes}")
        if not algum:
            w("  Nenhum arquivo com campos nulos.")
        w()

    log(f"Relatório salvo em: {ARQUIVO_RELATORIO}")
    log("ETAPA 2 concluída.")


# ============================================================
# ETAPA 3 — SQLITE -> JSON
# ============================================================
def etapa3_sqlite_para_json():
    log("=" * 70)
    log("ETAPA 3 — SQLite -> JSON")
    log("=" * 70)

    if not DB_PATH.exists():
        raise FileNotFoundError(f"Banco não encontrado: {DB_PATH}")

    sql_por_produto = """
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

    conn = sqlite3.connect(DB_PATH)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(gnv)").fetchall()]
        obrigatorias = ["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA"]
        faltantes = [c for c in obrigatorias if c not in cols]
        if faltantes:
            raise RuntimeError(
                f"Colunas faltando em gnv: {faltantes}. Colunas atuais: {cols}."
            )

        frames = []
        for produto in PRODUTOS:
            df = pd.read_sql_query(sql_por_produto, conn, params=(produto,))
            log(f"  {produto:<12} -> {len(df):>6} linha(s)")
            frames.append(df)
    finally:
        conn.close()

    if not frames:
        log("Nenhum registro retornado. Abortando geração do JSON.", "WARN")
        return

    df_final = pd.concat(frames, ignore_index=True)
    df_final["VL_VENDA_MEDIO"] = df_final["VL_VENDA_MEDIO"].round(4)
    df_final = df_final.sort_values(
        ["ESTADO", "PRODUTO", "MES_ANO"]
    ).reset_index(drop=True)

    if df_final.empty:
        log("Consolidado vazio. Abortando geração do JSON.", "WARN")
        return

    payload = {
        "metadados": {
            "titulo": "Preços médios de venda de DIESEL S10, DIESEL e GNV por MES e ESTADO",
            "data_de_atualizacao": datetime.now().strftime("%d/%m/%Y"),
            "responsavel": RESPONSAVEL,
            "nome_programa": NOME_PROGRAMA,
            "produtos": "DIESEL S10, DIESEL e GNV",
        },
        "dados": df_final.to_dict(orient="records"),
    }

    PASTA_ETL.mkdir(parents=True, exist_ok=True)
    with open(ARQUIVO_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    tamanho = ARQUIVO_JSON.stat().st_size / 1024
    log(f"JSON gerado: {ARQUIVO_JSON} ({tamanho:.1f} KB)")
    log(f"Total de registros: {len(df_final):,}".replace(",", "."))
    log("ETAPA 3 concluída.")


# ============================================================
# MAIN
# ============================================================
def main():
    log("=" * 78)
    log(f"INÍCIO DO PIPELINE — {NOME_PROGRAMA}")
    log(f"Projeto:  {BASE}")
    log(f"Bronze:   {PASTA_BRONZE}")
    log(f"Prata:    {PASTA_PRATA}")
    log(f"Banco:    {DB_PATH}")
    log(f"JSON:     {ARQUIVO_JSON}")
    log("=" * 78)

    try:
        etapa1_bronze_para_prata()
        etapa2_prata_para_sqlite()
        etapa3_sqlite_para_json()
    except SystemExit:
        raise
    except Exception as e:
        log(f"ERRO CRÍTICO: {e}", "ERROR")
        sys.exit(1)

    log("=" * 78)
    log("PIPELINE CONCLUÍDO COM SUCESSO.")
    log("=" * 78)


if __name__ == "__main__":
    main()