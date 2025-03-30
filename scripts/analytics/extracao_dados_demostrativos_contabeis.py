import os
import zipfile
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME", "ans_data")
DB_USER = os.getenv("DB_USER", "ans_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123456")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

PASTA_ZIP = "../arquivos/demonstracoes_contabeis"
PASTA_EXTRAIDA = "../arquivos/demonstracoes_contabeis/temp"
LOG_FILE = "erros_importacao.log"

# Criar pasta de extração se não existir
os.makedirs(PASTA_EXTRAIDA, exist_ok=True)

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

def extrair_arquivos_zip(pasta_zip, pasta_destino):
    arquivos_extraidos = []
    for arquivo in os.listdir(pasta_zip):
        if arquivo.endswith(".zip"):
            caminho_zip = os.path.join(pasta_zip, arquivo)
            try:
                with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                    zip_ref.extractall(pasta_destino)
                print(f"📂 Arquivo extraído: {arquivo}")
                arquivos_extraidos.extend(zip_ref.namelist())
            except Exception as e:
                print(f"❌ Erro ao extrair {arquivo}: {e}")

    return arquivos_extraidos

def criar_tabela(conn, df, tabela_destino):
    colunas = df.columns
    colunas_definicoes = [f'"{coluna}" TEXT' for coluna in colunas]
    colunas_definicoes_str = ", ".join(colunas_definicoes)
    create_table_query = f'CREATE TABLE IF NOT EXISTS {tabela_destino} ({colunas_definicoes_str});'

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
        conn.commit()
        print(f"✅ Tabela {tabela_destino} criada com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao criar a tabela: {e}")
        conn.rollback()

def processar_arquivos_csv(pasta_csv, tabela_destino, delimiter=";"):
    arquivos_csv = [f for f in os.listdir(pasta_csv) if f.endswith(".csv")]

    if not arquivos_csv:
        print(f"⚠️ Nenhum arquivo CSV encontrado em {pasta_csv}.")
        return

    conn = conectar_banco()
    if conn is None:
        return

    cursor = conn.cursor()

    for nome_arquivo in arquivos_csv:
        caminho_arquivo = os.path.join(pasta_csv, nome_arquivo)
        print(f"📂 Processando: {caminho_arquivo}")

        try:
            # Ler CSV tentando diferentes encodings
            encodings = ["utf-8", "latin1", "ISO-8859-1"]
            for enc in encodings:
                try:
                    df = pd.read_csv(caminho_arquivo, delimiter=delimiter, encoding=enc)
                    break  # Sai do loop se a leitura for bem-sucedida
                except UnicodeDecodeError:
                    continue

            df.columns = [col.strip().lower() for col in df.columns]

            criar_tabela(conn, df, tabela_destino)

            df = df.where(pd.notna(df), None)

            inserir_dados(cursor, conn, df, tabela_destino)

        except Exception as e:
            print(f"❌ Erro ao processar {nome_arquivo}: {e}")
            with open(LOG_FILE, "a") as log:
                log.write(f"Erro no arquivo {nome_arquivo}: {e}\n")

    conn.commit()
    print(f"✅ Dados da pasta {pasta_csv} foram inseridos com sucesso!")

    cursor.close()
    conn.close()

def inserir_dados(cursor, conn, df, tabela_destino):
    colunas = ", ".join(df.columns)
    valores = ", ".join(["%s"] * len(df.columns))
    query = f"""
        INSERT INTO {tabela_destino} ({colunas})
        VALUES ({valores})
        ON CONFLICT DO NOTHING;
    """

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    for i, row in enumerate(data):
        try:
            cursor.execute(query, row)
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print(f"⚠️ ERRO NA LINHA {i + 1}: Chave duplicada - {e}")
            with open(LOG_FILE, "a") as log:
                log.write(f"Linha {i + 1}: Chave duplicada - {e}\n")
        except psycopg2.DataError as e:
            conn.rollback()
            print(f"⚠️ ERRO NA LINHA {i + 1}: Erro de tipo de dado - {e}")
            with open(LOG_FILE, "a") as log:
                log.write(f"Linha {i + 1}: Erro de tipo de dado - {e}\n")
        except Exception as e:
            conn.rollback()
            print(f"⚠️ ERRO NA LINHA {i + 1}: {e}")
            with open(LOG_FILE, "a") as log:
                log.write(f"Linha {i + 1}: {e}\n")

if __name__ == "__main__":
    arquivos_extraidos = extrair_arquivos_zip(PASTA_ZIP, PASTA_EXTRAIDA)
    if arquivos_extraidos:
        processar_arquivos_csv(PASTA_EXTRAIDA, "tb_demonstracoes_contabeis")

    for arquivo in os.listdir(PASTA_EXTRAIDA):
        caminho_arquivo = os.path.join(PASTA_EXTRAIDA, arquivo)
        os.remove(caminho_arquivo)
    print("🗑️ Arquivos temporários removidos!")