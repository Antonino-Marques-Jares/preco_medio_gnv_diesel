"""
etl_gnv.py — Carga Prata -> SQLite (tabela gnv) + relatório de qualidade.

Regras de negócio:
    - Todas as linhas do CSV vão para a tabela `gnv` (exceto linhas 100% vazias).
    - Campos que não convertem / estão vazios viram NULL no banco.
    - Gera relatório de qualidade de dados com totais de nulos por campo,
      por arquivo e por motivo.

Saídas:
    - Banco:      gnv.db (tabela gnv)
    - Log:        logs/etl_sqlite_gnv_<timestamp>.log
    - Relatório:  relatorio_de_qualidade_de_dados.txt (no diretório do projeto)
"""

import pandas as pd
import sqlite3
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ============================================================
# CONFIG
# ============================================================
PASTA_PROJETO = Path(r"C:\Users\Nino\ProjetosPy\preco_medio_gnv_diesel")
PASTA_LOG = PASTA_PROJETO / "logs"
PASTA_LOG.mkdir(parents=True, exist_ok=True)

PASTA_PRATA = Path(r"D:\ETL\nivel_02_prata\diesel_gnv")
db_path = PASTA_PROJETO / "gnv.db"

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
arquivo_log = PASTA_LOG / f"etl_sqlite_gnv_{timestamp}.log"
arquivo_relatorio = PASTA_PROJETO / "relatorio_de_qualidade_de_dados.txt"

CAMPOS = ["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA"]

log_linhas = []


def log(mensagem: str, nivel: str = "INFO"):
    linha = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{nivel}] {mensagem}"
    print(linha, flush=True)
    log_linhas.append(linha)
    with open(arquivo_log, "a", encoding="utf-8") as f:
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


def preparar_banco(conn):
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


# ============================================================
# ESTATÍSTICAS
# ============================================================
stats = {
    "arquivos_lidos": 0,
    "linhas_lidas": 0,
    "linhas_inseridas": 0,
    "linhas_vazias_ignoradas": 0,
    "erros": 0,
}

# Totais de nulos por campo (global)
nulos_campo_global = {c: 0 for c in CAMPOS}

# Nulos por arquivo: arquivo -> {"total": N, "campos": {CAMPO: qtd_nulo}}
nulos_por_arquivo = {}

# Nulos por motivo em VL_VENDA
nulos_motivo_vl_venda = defaultdict(int)
nulos_valor_original_vl_venda = defaultdict(int)


# ============================================================
# EXECUÇÃO
# ============================================================
conn = None
cursor = None

try:
    arquivos = sorted(
        a for a in PASTA_PRATA.glob("precos-diesel-gnv-*.csv")
        if "resumo" not in a.name.lower()
    )
    log(f"{len(arquivos)} arquivo(s) mensal(is) encontrado(s) em {PASTA_PRATA}")
    if not arquivos:
        log("Pasta sem arquivos compatíveis. Abortando.", "ERROR")
        raise SystemExit(1)

    validar_cobertura(arquivos)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    log(f"Conectado ao banco: {db_path}")

    preparar_banco(conn)

    for arquivo in arquivos:
        try:
            df = pd.read_csv(arquivo, sep=",", encoding="latin1", low_memory=False)

            # Remove somente linhas 100% vazias (rodapé)
            antes = len(df)
            df = df.dropna(how="all")
            vazias = antes - len(df)
            if vazias:
                stats["linhas_vazias_ignoradas"] += vazias
                log(f"{arquivo.name}: {vazias} linha(s) totalmente vazia(s) ignorada(s)")

            faltantes = [c for c in CAMPOS if c not in df.columns]
            if faltantes:
                log(f"{arquivo.name}: colunas faltantes {faltantes}", "ERROR")
                stats["erros"] += 1
                continue

            total_arquivo = len(df)
            stats["linhas_lidas"] += total_arquivo

            df = df[CAMPOS].copy()

            # --- contabiliza nulos ANTES da conversão (texto vazio/conta como nulo) ---
            campos_nulos_arq = {}
            for c in CAMPOS:
                if c == "VL_VENDA":
                    continue  # tratado separadamente abaixo, com motivo
                # nulo = NaN original OU string vazia após strip
                serie_str = df[c].astype(str).str.strip()
                nulos = int((df[c].isna() | (serie_str == "") | (serie_str.str.lower() == "nan")).sum())
                campos_nulos_arq[c] = nulos
                nulos_campo_global[c] += nulos

            # --- VL_VENDA: normaliza e conta nulos com motivo ---
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

            # --- carga: TODAS as linhas; VL_VENDA NULL quando inválido ---
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

except SystemExit:
    pass
except Exception as e:
    log(f"ERRO CRÍTICO: {e}", "ERROR")

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


# ============================================================
# RELATÓRIO DE QUALIDADE DE DADOS
# ============================================================
def _num(n):
    return f"{n:,}".replace(",", ".")


with open(arquivo_relatorio, "w", encoding="utf-8") as f:
    def w(s=""):
        f.write(str(s) + "\n")

    w("=" * 78)
    w("RELATÓRIO DE QUALIDADE DE DADOS — ETL GNV")
    w(f"Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    w(f"Pasta Prata: {PASTA_PRATA}")
    w(f"Arquivos processados: {stats['arquivos_lidos']}")
    w("=" * 78)
    w()

    # ---------- Seção 1: resumo geral ----------
    w("─" * 78)
    w("1) RESUMO GERAL")
    w("─" * 78)
    w(f"  Linhas lidas:                    {_num(stats['linhas_lidas'])}")
    w(f"  Linhas inseridas na tabela gnv:  {_num(stats['linhas_inseridas'])}")
    w(f"  Linhas totalmente vazias:        {_num(stats['linhas_vazias_ignoradas'])} (ignoradas)")
    w(f"  Erros de processamento:          {_num(stats['erros'])}")
    w()

    # ---------- Seção 2: totais de nulos por campo ----------
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

    # ---------- Seção 3: nulos em VL_VENDA por motivo ----------
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

    # ---------- Seção 4: top valores originais nulos em VL_VENDA ----------
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

    # ---------- Seção 5: nulos por arquivo (todos os campos) ----------
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

    # ---------- Seção 6: arquivos com qualquer nulo (destaque) ----------
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

print()
print(f"Relatório salvo em: {arquivo_relatorio}")