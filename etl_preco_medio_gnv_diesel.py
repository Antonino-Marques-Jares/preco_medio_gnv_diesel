"""
Pipeline ETL — Preços médios de GNV/Diesel (Bronze -> Prata -> SQLite -> JSON -> Limpeza).

Etapas (todas em um único processo):
    passo01()  Bronze -> Prata: filtra e padroniza CSVs
    passo02()  Prata  -> SQLite (gnv.db) + relatório de qualidade
    passo03()  SQLite -> JSON de preços médios por estado
    passo04()  Limpeza: apaga gnv.db e CSVs da pasta prata

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
# CONFIG GLOBAL
# ============================================================
BASE = Path(r"C:\Users\Nino\ProjetosPy\preco_medio_gnv_diesel")

PASTA_BRONZE = Path(r"D:\ETL\nivel_01_bronze\diesel_gnv")
PASTA_PRATA = Path(r"D:\ETL\nivel_02_prata\diesel_gnv")

PASTA_LOG = BASE / "logs"
PASTA_ETL = BASE / "etl"
ARQUIVO_RELATORIO = BASE / "relatorio_de_qualidade_de_dados.txt"
DB_PATH = BASE / "gnv.db"
ARQUIVO_JSON = PASTA_ETL / "preco_medio_gnv_diesel_por_estado.json"

PRODUTOS = ["DIESEL S10", "DIESEL", "GNV"]
RESPONSAVEL = "Antonino Marques Jares"
NOME_PROGRAMA = "etl_preco_medio_gnv_diesel.py"

CAMPOS = ["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA"]

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)


# ============================================================
# PASSO 01 — Bronze -> Prata
# ============================================================
COLUNAS_NECESSARIAS_01 = [
    "Regiao - Sigla",
    "Estado - Sigla",
    "Municipio",
    "CNPJ da Revenda",
    "Produto",
    "Data da Coleta",
    "Valor de Venda",
]

RENOMEAR_01 = {
    "Estado - Sigla": "ESTADO",
    "Municipio": "MUNICIPIO",
    "CNPJ da Revenda": "CNPJ",
    "Produto": "PRODUTO",
    "Data da Coleta": "DATA",
    "Valor de Venda": "VL_VENDA",
}


def _ler_csv_com_fallback(caminho: Path, sep: str = ";") -> pd.DataFrame:
    """Tenta utf-8, latin1, cp1252 em sequência."""
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            return pd.read_csv(caminho, sep=sep, encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Não foi possível decodificar {caminho} (utf-8/latin1/cp1252)")


def passo01() -> int:
    print("\n=== passo01 — Bronze -> Prata ===")
    PASTA_PRATA.mkdir(parents=True, exist_ok=True)

    erros = 0
    processados = 0

    for ano in range(2022, 2027):
        for mes in [f"{m:02d}" for m in range(1, 13)]:
            origem = PASTA_BRONZE / f"precos-diesel-gnv-{ano}-{mes}.csv"
            destino = PASTA_PRATA / f"precos-diesel-gnv-{ano}-{mes}.csv"

            if not origem.exists():
                print(f"⚠ Arquivo não encontrado: {origem}")
                continue

            try:
                df = _ler_csv_com_fallback(origem, sep=";")

                faltantes = [c for c in COLUNAS_NECESSARIAS_01 if c not in df.columns]
                if faltantes:
                    print(f"⚠ Colunas faltantes em {origem.name}: {faltantes}")
                    erros += 1
                    continue

                df = df[COLUNAS_NECESSARIAS_01].rename(columns=RENOMEAR_01)

                df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")
                df["ANO"] = df["DATA"].dt.year.astype("Int64")
                df["MES"] = df["DATA"].dt.month.astype("Int64")
                df["MES_ANO"] = (
                    df["MES"].astype("string").str.zfill(2)
                    + "/"
                    + df["ANO"].astype("string")
                )

                df.to_csv(destino, index=False, encoding="utf-8")
                processados += 1
                print(f"✅ {destino.name} salvo ({len(df)} linhas)")

            except Exception as e:
                print(f"❌ Erro em {origem.name}: {e}")
                erros += 1

    print(f"Resumo passo01: {processados} arquivo(s) processado(s), {erros} erro(s).")
    return 1 if erros > 0 else 0


# ============================================================
# PASSO 02 — Prata -> SQLite + relatório de qualidade
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


def _validar_cobertura(arquivos, log):
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


def _preparar_banco(conn, log):
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


def passo02() -> int:
    print("\n=== passo02 — Prata -> SQLite + relatório ===")
    PASTA_LOG.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_log = PASTA_LOG / f"etl_sqlite_gnv_{timestamp}.log"

    def log(mensagem: str, nivel: str = "INFO"):
        linha = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{nivel}] {mensagem}"
        print(linha, flush=True)
        with open(arquivo_log, "a", encoding="utf-8") as f:
            f.write(linha + "\n")

    # --- estado local (não acumula entre execuções) ---
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

    conn = None
    cursor = None
    codigo_saida = 0

    try:
        arquivos = sorted(PASTA_PRATA.glob("precos-diesel-gnv-*.csv"))
        log(f"{len(arquivos)} arquivo(s) encontrado(s) em {PASTA_PRATA}")
        if not arquivos:
            log("Pasta sem arquivos compatíveis. Abortando.", "ERROR")
            return 1

        _validar_cobertura(arquivos, log)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        log(f"Conectado ao banco: {DB_PATH}")

        _preparar_banco(conn, log)

        for arquivo in arquivos:
            try:
                df = pd.read_csv(arquivo, sep=",", encoding="utf-8", low_memory=False)

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

                df["VL_VENDA_NUM"] = _normalizar_valor(df["VL_VENDA"])
                mask_nulo_vl = df["VL_VENDA_NUM"].isna()
                qtd_nulos_vl = int(mask_nulo_vl.sum())
                campos_nulos_arq["VL_VENDA"] = qtd_nulos_vl
                nulos_campo_global["VL_VENDA"] += qtd_nulos_vl

                if qtd_nulos_vl > 0:
                    for v in df.loc[mask_nulo_vl, "VL_VENDA"]:
                        nulos_motivo_vl_venda[_classificar_motivo(v)] += 1
                        nulos_valor_original_vl_venda[_to_str(v)[:60]] += 1

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

        total = cursor.execute("SELECT COUNT(*) FROM gnv").fetchone()[0]
        log(f"Total de registros na tabela gnv: {total}")

        if stats["linhas_inseridas"] == 0:
            log("Nenhuma linha inserida. Considerando falha.", "ERROR")
            codigo_saida = 1
        elif stats["erros"] > 0:
            log(f"{stats['erros']} erro(s) de processamento. Considerando falha.", "ERROR")
            codigo_saida = 1

    except Exception as e:
        log(f"ERRO CRÍTICO: {e}", "ERROR")
        codigo_saida = 1

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
        log("RESUMO FINAL")
        log(f"  Arquivos lidos:            {stats['arquivos_lidos']}")
        log(f"  Linhas lidas:              {stats['linhas_lidas']}")
        log(f"  Linhas inseridas:          {stats['linhas_inseridas']}")
        log(f"  Linhas vazias ignoradas:   {stats['linhas_vazias_ignoradas']}")
        log("  Nulos por campo:")
        for c in CAMPOS:
            log(f"    {c:<10} {nulos_campo_global[c]}")
        log(f"  Erros:                     {stats['erros']}")
        log("=" * 70)
        log("FIM")

    # ---------- relatório de qualidade ----------
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
            for valor, qtd in sorted(
                nulos_valor_original_vl_venda.items(), key=lambda x: -x[1]
            )[:30]:
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
            c = nulos_por_arquivo[arq]["campos"]
            if any(c[k] > 0 for k in CAMPOS):
                algum = True
                detalhes = ", ".join(f"{k}={_num(c[k])}" for k in CAMPOS if c[k] > 0)
                w(f"  {arq}  ->  {detalhes}")
        if not algum:
            w("  Nenhum arquivo com campos nulos.")
        w()

    print(f"Relatório salvo em: {ARQUIVO_RELATORIO}")
    return codigo_saida


# ============================================================
# PASSO 03 — SQLite -> JSON
# ============================================================
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


def _validar_schema_03(conn):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(gnv)").fetchall()]
    obrigatorias = ["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA"]
    faltantes = [c for c in obrigatorias if c not in cols]
    if faltantes:
        raise RuntimeError(
            f"Colunas faltando em gnv: {faltantes}. Colunas atuais: {cols}."
        )
    return cols


def _rodar_queries_03(conn):
    frames = []
    for produto in PRODUTOS:
        df = pd.read_sql_query(SQL_POR_PRODUTO, conn, params=(produto,))
        print(f"  {produto:<12} -> {len(df):>6} linha(s)")
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df_final = pd.concat(frames, ignore_index=True)
    df_final["VL_VENDA_MEDIO"] = df_final["VL_VENDA_MEDIO"].round(4)
    return df_final.sort_values(["ESTADO", "PRODUTO", "MES_ANO"]).reset_index(drop=True)


def passo03() -> int:
    print("\n=== passo03 — SQLite -> JSON ===")

    if not DB_PATH.exists():
        print(f"[ERRO] Banco não encontrado: {DB_PATH}")
        print("       Verifique se o passo02 rodou com sucesso e gerou gnv.db.")
        return 1

    print(f"Banco: {DB_PATH}")
    print(f"Produtos: {PRODUTOS}")

    conn = sqlite3.connect(DB_PATH)
    try:
        _validar_schema_03(conn)
        total = conn.execute("SELECT COUNT(*) FROM gnv").fetchone()[0]
        print(f"Total de registros em gnv: {total:,}".replace(",", "."))
        if total == 0:
            print("[ERRO] Tabela gnv vazia. Abortando.")
            return 1

        print("\nRodando as 3 queries (por ESTADO):")
        df = _rodar_queries_03(conn)
    finally:
        conn.close()

    if df.empty:
        print("\n[ERRO] Nenhum registro retornado pelas queries.")
        print("       Verifique: SELECT DISTINCT PRODUTO FROM gnv;")
        return 1

    print(f"\nTotal consolidado: {len(df):,} linha(s)".replace(",", "."))
    print("\nAmostra:")
    print(df.head(10).to_string(index=False))

    payload = {
        "metadados": {
            "titulo": "Preços médios de venda de DIESEL S10, DIESEL e GNV por MES e ESTADO",
            "data_de_atualizacao": datetime.now().strftime("%d/%m/%Y"),
            "responsavel": RESPONSAVEL,
            "nome_programa": NOME_PROGRAMA,
            "produtos": "DIESEL S10, DIESEL e GNV",
        },
        "dados": df.to_dict(orient="records"),
    }

    PASTA_ETL.mkdir(parents=True, exist_ok=True)
    with open(ARQUIVO_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    tamanho = ARQUIVO_JSON.stat().st_size / 1024
    print(f"\nJSON gerado: {ARQUIVO_JSON} ({tamanho:.1f} KB)")
    return 0


# ============================================================
# PASSO 04 — Limpeza
# ============================================================
def _remover_arquivo(caminho: Path) -> bool:
    try:
        caminho.unlink()
        print(f"🗑  Removido: {caminho}")
        return True
    except FileNotFoundError:
        print(f"ℹ  Não existe (ok): {caminho}")
        return True
    except PermissionError:
        print(f"❌ Sem permissão (arquivo em uso?): {caminho}")
        return False
    except Exception as e:
        print(f"❌ Erro ao remover {caminho}: {e}")
        return False


def passo04() -> int:
    print("\n=== passo04 — Limpeza de artefatos ===")

    ok = True

    # banco
    print("--- Banco SQLite ---")
    if DB_PATH.exists():
        ok = _remover_arquivo(DB_PATH) and ok
    else:
        print(f"ℹ  Banco não existe (ok): {DB_PATH}")

    # CSVs da prata
    print("--- CSVs da pasta prata ---")
    if not PASTA_PRATA.exists():
        print(f"ℹ  Pasta não existe (ok): {PASTA_PRATA}")
    else:
        csvs = sorted(PASTA_PRATA.glob("*.csv"))
        if not csvs:
            print(f"ℹ  Nenhum CSV encontrado em: {PASTA_PRATA}")
        else:
            print(f"   {len(csvs)} CSV(s) encontrado(s).")
            for arq in csvs:
                ok = _remover_arquivo(arq) and ok

    return 0 if ok else 1


# ============================================================
# ORQUESTRADOR
# ============================================================
PASSOS = [
    ("passo01", "Bronze -> Prata: filtra e padroniza CSV"),
    ("passo02", "Prata -> SQLite (gnv.db) + relatório de qualidade"),
    ("passo03", "SQLite -> JSON de preços médios por estado"),
    ("passo04", "Limpeza de artefatos gerados pelo pipeline"),
]


def main() -> int:
    for nome, descricao in PASSOS:
        print(f"\n{'=' * 78}\n{nome} — {descricao}\n{'=' * 78}")
        funcao = globals().get(nome)
        if not callable(funcao):
            print(f"[ERRO] Função {nome}() não encontrada. Abortando.")
            return 1

        code = funcao()
        if code != 0:
            print(f"\n[ERRO] {nome}() retornou código {code}. Abortando pipeline.")
            return code

    print("\n✅ Todos os passos executados com sucesso.")
    return 0


if __name__ == "__main__":
    sys.exit(main())