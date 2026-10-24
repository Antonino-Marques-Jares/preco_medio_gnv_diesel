import subprocess
import sys
from pathlib import Path

BASE = Path(r"C:\Users\Nino\ProjetosPy\preco_medio_gnv_diesel")


def rodar(nome):
    print(f"\n>>> Executando {nome}")
    return subprocess.run([sys.executable, str(BASE / nome)], cwd=str(BASE)).returncode


if __name__ == "__main__":
    """
    Após salvar os arquivos csv e nomealos no padrão precos-diesel-gnv-{ano}-{mes}.csv em
    D:\\ETL\\nivel_01_bronze\\diesel_gnv
    este programa irá criar novos csv's com as colunas que importam e salvar em 
    D:\\ETL\\nivel_02_prata\\diesel_gnv
    """
    if rodar("passo01.py") != 0:
        print("ERRO em passo01.py. Abortando.")
        sys.exit(1)

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
    if rodar("passo02.py") != 0:
        print("ERRO em passo02.py. Abortando.")
        sys.exit(1)

    
    """
    gerar_json_por_estado.py — Roda 3 queries de média de VL_VENDA por
    ESTADO, PRODUTO e MES_ANO (DIESEL S10, DIESEL, GNV), consolida e salva
    em preco_medio_gnv_diesel_por_estado.json.

    Uso:
        python gerar_json_por_estado.py
    """
    if rodar("passo03.py") != 0:
        print("ERRO em passo03.py. Abortando.")
        sys.exit(1)

    print("\nTodos os passos executados com sucesso.")