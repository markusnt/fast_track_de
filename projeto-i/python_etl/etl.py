import pandas as pd
import psycopg2
from pymongo import MongoClient

# ----- CONFIGURACOES -----

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "pb_dw",
    "user": "postgres",
    "password": "postgres"
}

MONGO_CONFIG = {
    "host": "mongodb",
    "port": 27017,
    "db": "admin",
    "collection": "order_reviews"
}

# ----- FUNCOES -----

def conectar_postgres():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("🟢 Conectado ao PostgreSQL com sucesso.")
        return conn
    except Exception as e:
        print("🔴 Erro ao conectar no PostgreSQL:", e)
        return None

def conectar_mongodb():
    try:
        client = MongoClient(MONGO_CONFIG["host"], MONGO_CONFIG["port"])
        db = client[MONGO_CONFIG["db"]]
        collection = db[MONGO_CONFIG["collection"]]
        print("🟢 Conectado ao MongoDB com sucesso.")
        return collection
    except Exception as e:
        print("🔴 Erro ao conectar no MongoDB:", e)
        return None

def criar_tabelas_postgres(conn):
    ddl_queries = [
        """
        CREATE TABLE IF NOT EXISTS dim_clientes (
            cliente_id VARCHAR PRIMARY KEY,
            estado VARCHAR
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_produtos (
            produto_id VARCHAR PRIMARY KEY,
            categoria VARCHAR
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_pagamentos (
            pagamento_id SERIAL PRIMARY KEY,
            tipo_pagamento VARCHAR,
            quantidade_parcelas INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_data (
            data DATE PRIMARY KEY,
            ano INT,
            mes INT,
            dia INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fato_pedidos (
            pedido_id VARCHAR PRIMARY KEY,
            cliente_id VARCHAR,
            produto_id VARCHAR,
            pagamento_id INT,
            data_compra DATE,
            valor_pago NUMERIC,
            score_review INT,
            FOREIGN KEY (cliente_id) REFERENCES dim_clientes(cliente_id),
            FOREIGN KEY (produto_id) REFERENCES dim_produtos(produto_id),
            FOREIGN KEY (pagamento_id) REFERENCES dim_pagamentos(pagamento_id),
            FOREIGN KEY (data_compra) REFERENCES dim_data(data)
        );
        """
    ]

    try:
        cur = conn.cursor()
        for ddl in ddl_queries:
            cur.execute(ddl)
        conn.commit()
        cur.close()
        print("✅ Tabelas criadas com sucesso no PostgreSQL.")
    except Exception as e:
        print("❌ Erro ao criar tabelas:", e)
def main():
    print("🚀 Iniciando o processo ETL...")

    pg_conn = conectar_postgres()
    mongo_collection = conectar_mongodb()

    if pg_conn is None or mongo_collection is None:
        print("❌ Abortando ETL por erro de conexão.")
        return

    print("📦 Lendo arquivos CSV...")

    try:
        df_customers = pd.read_csv("input/olist_customers_dataset.csv")
        df_orders = pd.read_csv("input/olist_orders_dataset.csv")
        df_order_items = pd.read_csv("input/olist_order_items_dataset.csv")
        df_order_payments = pd.read_csv("input/olist_order_payments_dataset.csv")
        df_products = pd.read_csv("input/olist_products_dataset.csv")

        print("🟢 Arquivos CSV carregados com sucesso.")
    except Exception as e:
        print("🔴 Erro ao ler arquivos CSV:", e)
        return

    # Criação das tabelas
    criar_tabelas_postgres(pg_conn)

    # ----------------------------
    # dim_clientes
    print("📥 Inserindo dados em dim_clientes...")

    try:
        cur = pg_conn.cursor()
        clientes = df_customers[['customer_id', 'customer_state']].drop_duplicates()
        for _, row in clientes.iterrows():
            cur.execute("""
                INSERT INTO dim_clientes (cliente_id, estado)
                VALUES (%s, %s)
                ON CONFLICT (cliente_id) DO NOTHING;
            """, (row['customer_id'], row['customer_state']))
        pg_conn.commit()
        cur.close()
        print("✅ dim_clientes populada com sucesso.")
    except Exception as e:
        print("❌ Erro ao inserir em dim_clientes:", e)

    # ----------------------------
    # dim_produtos
    print("📥 Inserindo dados em dim_produtos...")

    try:
        cur = pg_conn.cursor()
        produtos = df_products[['product_id', 'product_category_name']].drop_duplicates()
        for _, row in produtos.iterrows():
            cur.execute("""
                INSERT INTO dim_produtos (produto_id, categoria)
                VALUES (%s, %s)
                ON CONFLICT (produto_id) DO NOTHING;
            """, (row['product_id'], row['product_category_name']))
        pg_conn.commit()
        cur.close()
        print("✅ dim_produtos populada com sucesso.")
    except Exception as e:
        print("❌ Erro ao inserir em dim_produtos:", e)
    # ----------------------------
    # dim_pagamentos
    print("📥 Inserindo dados em dim_pagamentos...")

    try:
        cur = pg_conn.cursor()
        pagamentos = df_order_payments[['payment_type', 'payment_installments']].drop_duplicates()
        for _, row in pagamentos.iterrows():
            cur.execute("""
                INSERT INTO dim_pagamentos (tipo_pagamento, quantidade_parcelas)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (row['payment_type'], row['payment_installments']))
        pg_conn.commit()
        cur.close()
        print("✅ dim_pagamentos populada com sucesso.")
    except Exception as e:
        print("❌ Erro ao inserir em dim_pagamentos:", e)

    # ----------------------------
    # dim_data
    print("📥 Inserindo dados em dim_data...")

    try:
        cur = pg_conn.cursor()
        datas = pd.to_datetime(df_orders['order_purchase_timestamp']).drop_duplicates()
        df_datas = pd.DataFrame()
        df_datas['data'] = datas
        df_datas['ano'] = df_datas['data'].dt.year
        df_datas['mes'] = df_datas['data'].dt.month
        df_datas['dia'] = df_datas['data'].dt.day

        for _, row in df_datas.iterrows():
            cur.execute("""
                INSERT INTO dim_data (data, ano, mes, dia)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (data) DO NOTHING;
            """, (row['data'], row['ano'], row['mes'], row['dia']))
        pg_conn.commit()
        cur.close()
        print("✅ dim_data populada com sucesso.")
    except Exception as e:
        print("❌ Erro ao inserir em dim_data:", e)
    # ----------------------------
    # fato_pedidos
    print("📥 Inserindo dados na fato_pedidos...")

    try:
        cur = pg_conn.cursor()

        # Agrupando os pagamentos por pedido
        pagamentos = df_order_payments.groupby('order_id').agg({
            'payment_type': 'first',
            'payment_installments': 'first',
            'payment_value': 'sum'
        }).reset_index()

        # Criar um dicionário para mapear pagamento para ID
        pagamento_id_map = {}

        for _, row in pagamentos.iterrows():
            cur.execute("""
                SELECT pagamento_id FROM dim_pagamentos
                WHERE tipo_pagamento = %s AND quantidade_parcelas = %s
            """, (row['payment_type'], row['payment_installments']))
            result = cur.fetchone()
            if result:
                pagamento_id_map[row['order_id']] = (result[0], row['payment_value'])

        # Processar reviews do MongoDB
        review_dict = {}
        for review in mongo_collection.find():
            review_dict[review['review_id']] = review.get('review_score', None)

        # Merge dos dados relevantes
        merged = df_orders.merge(df_order_items, on='order_id')
        merged = merged[merged['order_id'].isin(pagamento_id_map)]

        for _, row in merged.iterrows():
            pedido_id = row['order_id']
            cliente_id = row['customer_id']
            produto_id = row['product_id']
            data_compra = pd.to_datetime(row['order_purchase_timestamp']).date()
            pagamento_id, valor_pago = pagamento_id_map[pedido_id]
            review_score = review_dict.get(row['order_id'], None)

            cur.execute("""
                INSERT INTO fato_pedidos (
                    pedido_id, cliente_id, produto_id, pagamento_id,
                    data_compra, valor_pago, score_review
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pedido_id) DO NOTHING;
            """, (
                pedido_id, cliente_id, produto_id, pagamento_id,
                data_compra, valor_pago, review_score
            ))

        pg_conn.commit()
        cur.close()
        print("✅ fato_pedidos populada com sucesso.")
    except Exception as e:
        print("❌ Erro ao inserir em fato_pedidos:", e)

    pg_conn.close()
    print("🏁 ETL finalizado com sucesso.")
    
if __name__ == "__main__":
    main()
