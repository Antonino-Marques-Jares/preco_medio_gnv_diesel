"""
gerar_json_por_estado.py — Roda 3 queries de média de VL_VENDA por
ESTADO, PRODUTO e MES_ANO (DIESEL S10, DIESEL, GNV), consolida e salva
em preco_medio_gnv_diesel_por_estado.json.

Uso:
    python gerar_json_por_estado.py
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd

# ============================================================
# CONFIG
# ============================================================
BASE_PATH = Path(r"C:\Users\Nino\ProjetosPy\preco_medio_gnv_diesel")
DB_PATH = BASE_PATH / "gnv.db"
ARQUIVO_JSON = BASE_PATH / "preco_medio_gnv_diesel_por_estado.json"

PRODUTOS = ["DIESEL S10", "DIESEL", "GNV"]

RESPONSAVEL = "Antonino Marques Jares"
NOME_PROGRAMA = "etl_atualizar_preco_medio_gnv_diesel_no_site.py"

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


# ============================================================
# EXECUÇÃO
# ============================================================
def validar_schema(conn):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(gnv)").fetchall()]
    obrigatorias = ["ESTADO", "PRODUTO", "MES_ANO", "VL_VENDA"]
    faltantes = [c for c in obrigatorias if c not in cols]
    if faltantes:
        raise RuntimeError(
            f"Colunas faltando em gnv: {faltantes}. "
            f"Colunas atuais: {cols}."
        )
    return cols


def rodar_queries(conn):
    frames = []
    resumo = []
    for produto in PRODUTOS:
        df = pd.read_sql_query(SQL_POR_PRODUTO, conn, params=(produto,))
        print(f"  {produto:<12} -> {len(df):>6} linha(s)")
        resumo.append({"produto": produto, "linhas": len(df)})
        frames.append(df)

    if not frames:
        return pd.DataFrame(), resumo

    df_final = pd.concat(frames, ignore_index=True)
    df_final["VL_VENDA_MEDIO"] = df_final["VL_VENDA_MEDIO"].round(4)
    df_final = df_final.sort_values(
        ["ESTADO", "PRODUTO", "MES_ANO"]
    ).reset_index(drop=True)
    return df_final, resumo


def gerar_json(df: pd.DataFrame, resumo_por_produto: list):
    ARQUIVO_JSON.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "metadados": {
            "titulo": "Preços médios de venda de DIESEL S10, DIESEL e GNV por MES e ESTADO",
            "data_de_atualizacao": datetime.now().strftime("%d/%m/%Y"),
            "responsavel": RESPONSAVEL,
            "nome_programa": NOME_PROGRAMA,
            "produtos": "DIESEL S10, DIESEL e GNV"
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
    print(f"Total de registros: {len(df):,}".replace(",", "."))
    return payload


# ============================================================
# MAIN
# ============================================================
def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Banco não encontrado: {DB_PATH}")

    print("=" * 78)
    print(f"Banco: {DB_PATH}")
    print(f"Produtos: {PRODUTOS}")
    print("=" * 78)

    conn = sqlite3.connect(DB_PATH)
    try:
        cols = validar_schema(conn)
        print(f"Colunas em gnv: {cols}\n")

        print("Rodando as 3 queries (por ESTADO):")
        df, resumo = rodar_queries(conn)
    finally:
        conn.close()

    if df.empty:
        print("\n[AVISO] Nenhum registro retornado.")
        print("        Verifique: SELECT DISTINCT PRODUTO FROM gnv;")
        return

    print(f"\nTotal consolidado: {len(df):,} linha(s)".replace(",", "."))
    print("\nAmostra:")
    print(df.head(10).to_string(index=False))

    gerar_json(df, resumo)


if __name__ == "__main__":
    main()