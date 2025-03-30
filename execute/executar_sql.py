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
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_SCRIPT_1 = os.path.join(BASE_DIR, "scripts/download_processamento/download_ans_data.py")
PYTHON_SCRIPT_2 = os.path.join(BASE_DIR, "scripts/analytics/extracao_dados_demostrativos_contabeis.py")
PYTHON_SCRIPT_3 = os.path.join(BASE_DIR, "scripts/analytics/extracao_operacoes.py")
SQL_FILE_1 = os.path.join(BASE_DIR, "scripts/ddl/01_select_despesas_trimestre.sql")
SQL_FILE_2 = os.path.join(BASE_DIR, "scripts/ddl/02_select_operadoras_ultimo_ano.sql")

# Verificar e criar diretório se necessário
DEMONSTRACOES_DIR = os.path.join(BASE_DIR, "scripts/download_processamento/data/demonstracoes_contabeis")
if not os.path.exists(DEMONSTRACOES_DIR):
    os.makedirs(DEMONSTRACOES_DIR)

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

# Executar script SQL e imprimir resultados
def executar_sql(conn, sql_file):
    try:
        with open(sql_file, 'r') as file:
            sql = file.read()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                print(row)
        conn.commit()
        print(f"✅ Script SQL {sql_file} executado com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao executar script SQL {sql_file}: {e}")
        conn.rollback()

# Executar script Python
def executar_python(script_path):
    try:
        print(f"🔄 Executando script Python: {script_path}")
        result = subprocess.run(["python3", script_path], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Script Python {script_path} executado com sucesso!")
            print(f"📄 Saída:\n{result.stdout}")
        else:
            print(f"❌ Erro ao executar script Python {script_path}: {result.stderr}")
    except Exception as e:
        print(f"❌ Erro ao executar script Python {script_path}: {e}")

# Executar scripts na sequência
def executar_scripts():
    # Executar scripts Python na ordem especificada
    executar_python(PYTHON_SCRIPT_1)
    executar_python(PYTHON_SCRIPT_2)
    executar_python(PYTHON_SCRIPT_3)

    # Conectar ao banco de dados para executar scripts SQL
    conn = conectar_banco()
    if conn:
        executar_sql(conn, SQL_FILE_1)
        executar_sql(conn, SQL_FILE_2)
        conn.close()

if __name__ == "__main__":
    executar_scripts()