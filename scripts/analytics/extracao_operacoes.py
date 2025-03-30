import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Configurações do banco de dados
DB_NAME = "ans_data"
DB_USER = "ans_user"
DB_PASSWORD = "123456"
DB_HOST = "localhost"
DB_PORT = 5432

# Pastas onde os arquivos CSV estão localizados
PASTA_OPERADORAS = "/home/leonidas/Área de Trabalho/intuitive/DataBase/scripts_download_processamento/data/operadoras_ativas"

# Conectar ao banco de dados
def conectar_banco():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("✅ Conectado ao banco de dados!")
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return None

# Processar arquivos CSV
def processar_pasta(caminho_pasta, tabela_destino, delimiter=";"):
    if not os.path.exists(caminho_pasta):
        print(f"❌ Pasta não encontrada: {caminho_pasta}")
        return

    arquivos_csv = [f for f in os.listdir(caminho_pasta) if f.endswith(".csv")]

    if not arquivos_csv:
        print(f"⚠️ Nenhum arquivo CSV encontrado em {caminho_pasta}.")
        return

    conn = conectar_banco()
    if conn is None:
        return

    cursor = conn.cursor()

    for nome_arquivo in arquivos_csv:
        caminho_arquivo = os.path.join(caminho_pasta, nome_arquivo)
        print(f"📂 Processando: {caminho_arquivo}")

        try:
            df = pd.read_csv(caminho_arquivo, delimiter=delimiter, encoding="utf-8")

            # Remover espaços extras dos nomes das colunas
            df.columns = [col.strip() for col in df.columns]

            # Substituir valores NaN por None
            df = df.where(pd.notna(df), None)

            # Tratar colunas que podem causar erro
            if "telefone" in df.columns:
                df["telefone"] = df["telefone"].astype(str).str[:15]  # Limite 15 caracteres
            if "fax" in df.columns:
                df["fax"] = df["fax"].astype(str).str[:15]
            if "cep" in df.columns:
                df["cep"] = df["cep"].astype(str).str[:10]
            if "regiao_de_comercializacao" in df.columns:
                df["regiao_de_comercializacao"] = df["regiao_de_comercializacao"].apply(lambda x: int(x) if pd.notna(x) and -9223372036854775808 <= x <= 9223372036854775807 else None)

            # Inserir dados no banco
            inserir_dados(cursor, df, tabela_destino)

        except Exception as e:
            print(f"❌ Erro ao processar {nome_arquivo}: {e}")

    conn.commit()
    print(f"✅ Dados da pasta {caminho_pasta} foram inseridos com sucesso!")
    cursor.close()
    conn.close()

# Função para inserir dados e tratar erros
def inserir_dados(cursor, df, tabela_destino):
    colunas = ", ".join(df.columns)
    valores = ", ".join(["%s"] * len(df.columns))
    query = f"""
        INSERT INTO {tabela_destino} ({colunas})
        VALUES ({valores})
        ON CONFLICT (id) DO UPDATE
        SET {", ".join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != "id"])};
    """

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    for i, row in enumerate(data):
        try:
            cursor.execute(query, row)
            print(f"✅ Linha {i + 1} inserida com sucesso!")

        except psycopg2.errors.UniqueViolation:
            print(f"❌ ERRO NA LINHA {i + 1}: Chave duplicada detectada.")
            cursor.connection.rollback()

        except psycopg2.errors.NumericValueOutOfRange as e:
            print(f"❌ ERRO NA LINHA {i + 1}: Número muito grande para a coluna. {e}")
            cursor.connection.rollback()

        except psycopg2.errors.NotNullViolation as e:
            print(f"❌ ERRO NA LINHA {i + 1}: Coluna obrigatória com valor NULL. {e}")
            cursor.connection.rollback()

        except Exception as e:
            print(f"❌ ERRO NA LINHA {i + 1}: {e}")
            cursor.connection.rollback()

# Executar o script
processar_pasta(PASTA_OPERADORAS, "tb_operadoras")