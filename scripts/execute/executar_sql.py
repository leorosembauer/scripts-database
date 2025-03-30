import os
import psycopg2
import subprocess
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Configurações do banco de dados
DB_NAME = os.getenv("DB_NAME", "ans_data")
DB_USER = os.getenv("DB_USER", "ans_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123456")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Caminhos dos arquivos
SQL_FILE = "/home/leonidas/Área de Trabalho/intuitive/DataBase/scripts/ddl/01_create_tables.sql"
PYTHON_SCRIPT_1 = "/home/leonidas/Área de Trabalho/intuitive/DataBase/scripts/analytics/extracao_dados_demostrativos_contabeis.py"
PYTHON_SCRIPT_2 = "/home/leonidas/Área de Trabalho/intuitive/DataBase/scripts/analytics/extracao_operacoes.py"

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

# Executar script SQL
def executar_sql(conn, sql_file):
    try:
        with open(sql_file, 'r') as file:
            sql = file.read()
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()
        print(f"✅ Script SQL {sql_file} executado com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao executar script SQL {sql_file}: {e}")
        conn.rollback()

# Executar script Python
def executar_python(script_path):
    try:
        result = subprocess.run(["python3", script_path], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Script Python {script_path} executado com sucesso!")
        else:
            print(f"❌ Erro ao executar script Python {script_path}: {result.stderr}")
    except Exception as e:
        print(f"❌ Erro ao executar script Python {script_path}: {e}")

# Executar scripts na sequência
def executar_scripts():
    conn = conectar_banco()
    if conn:
        executar_sql(conn, SQL_FILE)
        conn.close()

    executar_python(PYTHON_SCRIPT_1)
    executar_python(PYTHON_SCRIPT_2)

# Executar a função principal
if __name__ == "__main__":
    executar_scripts()