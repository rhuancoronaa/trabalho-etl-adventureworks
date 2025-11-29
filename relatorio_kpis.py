import pandas as pd
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def rodar_kpis():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    
    print("="*50)
    print("RELATORIO DE PERFORMANCE - ADVENTUREWORKS DW")
    print("="*50)
    print("\n")

    print(">>> 1. Faturamento Bruto Total")
    sql1 = "SELECT SUM(valor_total) AS faturamento_bruto FROM fato_vendas;"
    print(pd.read_sql(sql1, engine))
    print("-" * 30)

    print(">>> 2. Quantidade Total de Itens Vendidos")
    sql2 = "SELECT SUM(quantidade) AS total_itens FROM fato_vendas;"
    print(pd.read_sql(sql2, engine))
    print("-" * 30)

    print(">>> 3. Ticket Medio")
    sql3 = "SELECT AVG(valor_total) AS ticket_medio FROM fato_vendas;"
    print(pd.read_sql(sql3, engine))
    print("-" * 30)

    print(">>> 4. Faturamento Liquido")
    sql4 = "SELECT SUM(valor_liquido) AS faturamento_liquido FROM fato_vendas;"
    print(pd.read_sql(sql4, engine))
    print("-" * 30)

    print(">>> 5. Vendas por Pais")
    sql5 = """
    SELECT c.pais, SUM(f.valor_total) AS total_vendas
    FROM fato_vendas f
    JOIN dim_cliente c ON f.id_cliente = c.id_cliente
    GROUP BY c.pais;
    """
    print(pd.read_sql(sql5, engine))
    print("-" * 30)

    print(">>> 6. Top 5 Produtos Mais Vendidos")
    sql6 = """
    SELECT p.nome_produto, SUM(f.valor_total) AS receita
    FROM fato_vendas f
    JOIN dim_produto p ON f.id_produto = p.id_produto
    GROUP BY p.nome_produto
    ORDER BY receita DESC
    LIMIT 5;
    """
    print(pd.read_sql(sql6, engine))
    print("-" * 30)

    print(">>> 7. Faturamento por Cor")
    sql7 = """
    SELECT p.cor, SUM(f.valor_total) AS total_vendas
    FROM fato_vendas f
    JOIN dim_produto p ON f.id_produto = p.id_produto
    GROUP BY p.cor;
    """
    print(pd.read_sql(sql7, engine))
    print("-" * 30)

    print(">>> 8. Top 5 Melhores Clientes")
    sql8 = """
    SELECT c.nome, c.sobrenome, SUM(f.valor_total) AS total_gasto
    FROM fato_vendas f
    JOIN dim_cliente c ON f.id_cliente = c.id_cliente
    GROUP BY c.nome, c.sobrenome
    ORDER BY total_gasto DESC
    LIMIT 5;
    """
    print(pd.read_sql(sql8, engine))
    print("-" * 30)

    print(">>> 9. Total de Pedidos Unicos")
    sql9 = "SELECT COUNT(DISTINCT id_venda) AS total_pedidos FROM fato_vendas;"
    print(pd.read_sql(sql9, engine))
    print("-" * 30)

    print(">>> 10. Vendas por Data")
    sql10 = """
    SELECT data_venda, SUM(valor_total) AS vendas_dia
    FROM fato_vendas
    GROUP BY data_venda
    ORDER BY data_venda;
    """
    print(pd.read_sql(sql10, engine))
    print("=" * 50)

if __name__ == "__main__":
    rodar_kpis()