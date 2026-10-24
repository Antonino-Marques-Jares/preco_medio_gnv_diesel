![Caminhão](Caminhão.jpg)

# Preço médio por Estado Diesel (comum ou S10) e GNV e eventos relevantes 
  Gráfico exibe informações dos preços médios em litro ou m³ e eventos relevantes

## Passo 1: 
  Clone o projeto pelo github

## Passo 2:
  atualize as bibliotecas
  pip install -r requirements.txt
  
## Passo 3:
  Execute python etl_atualizar_preco_medio_gnv_diesel_no_site.py


# Resultado é um JSON que calcula a média agrupado por produto, estado, mes_ano

Executa 3 vezes a SQL a seguir um para dada produto e depois une os 3 resultados e 1 json
  
```sql
SELECT
    ESTADO,
    PRODUTO,
    MES_ANO,
    AVG(VL_VENDA) AS VL_VENDA_MEDIO
FROM gnv
WHERE PRODUTO = ?
GROUP BY ESTADO, MES_ANO
ORDER BY ESTADO, MES_ANO
```
  

# Saiba mais em: 
[Ativo - Área de Trampo](https://www.areadetrampo.com.br/ativo/)

