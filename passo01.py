import pandas as pd
from pathlib import Path
import warnings

"""
Após salvar os arquivos csv e nomealos no padrão precos-diesel-gnv-{ano}-{mes}.csv em
D:\\ETL\\nivel_01_bronze\\diesel_gnv
este programa irá criar novos csv's com as colunas que importam e salvar em 
D:\\ETL\\nivel_02_prata\\diesel_gnv
"""

# Suprime avisos de dtype (opcional, mas recomendado para evitar poluição no output)
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# Lista de anos (2022 a 2026)
anos = list(range(2022, 2027))

for ano in anos:
    for mes in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
        try:
            # Define caminhos dos arquivos
            planilha_origem = Path(f"D:\\ETL\\nivel_01_bronze\\diesel_gnv\\precos-diesel-gnv-{ano}-{mes}.csv")
            planilha_destino = Path(f"D:\\ETL\\nivel_02_prata\\diesel_gnv\\precos-diesel-gnv-resumo-{ano}-{mes}.csv")

            # Verifica se o arquivo existe
            if not planilha_origem.exists():
                print(f"⚠ Arquivo não encontrado: {planilha_origem}")
                continue  # Pula para o próximo arquivo

            # Tenta ler com diferentes codificações e ignora dtype misto
            try:
                df = pd.read_csv(planilha_origem, sep=";", encoding='utf-8', low_memory=False)
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(planilha_origem, sep=";", encoding='latin1', low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(planilha_origem, sep=";", encoding='cp1252', low_memory=False)

            # Verifica se as colunas necessárias existem
            colunas_necessarias = [
                'Regiao - Sigla', 'Estado - Sigla', 'Municipio',
                'CNPJ da Revenda', 'Produto', 'Data da Coleta', 'Valor de Venda'
            ]
            
            colunas_faltantes = [col for col in colunas_necessarias if col not in df.columns]
            if colunas_faltantes:
                print(f"⚠ Colunas faltantes em {planilha_origem}: {colunas_faltantes}")
                continue  # Pula se não tiver todas as colunas

            # Seleciona e renomeia colunas
            df = df[colunas_necessarias].rename(columns={
                'Estado - Sigla': 'ESTADO',
                'Municipio': 'MUNICIPIO',
                'CNPJ da Revenda': 'CNPJ',
                'Produto': 'PRODUTO',
                'Data da Coleta': 'DATA',
                'Valor de Venda': 'VL_VENDA'
            })

            # Processa datas (converte e extrai ano/mês)
            df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
            df['ANO'] = df['DATA'].dt.year.astype('Int64')
            df['MES'] = df['DATA'].dt.month.astype('Int64')
            # Formato 01/04 em vez de Jan/04
            df['MES_ANO'] = df['MES'].astype(str).str.zfill(2) + '/' + df['ANO'].astype(str)

            # Salva o arquivo processado
            df.to_csv(planilha_destino, index=False, encoding='utf-8')
            print(f"✅ {planilha_destino} salvo com sucesso!")

        except Exception as e:
            print(f"❌ Erro grave em {planilha_origem}: {str(e)}")

print("✅ Todos os arquivos processados!")