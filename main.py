from fastapi import FastAPI, HTTPException
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Configuração do banco de dados
DB_NAME = os.getenv("DB_NAME", "ans_data")
DB_USER = os.getenv("DB_USER", "ans_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123456")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)

# Conectar ao banco
def conectar_banco():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

@app.get("/operadoras")
def listar_operadoras(filtro: str = None):
    try:
        conn = conectar_banco()
        cursor = conn.cursor()

        query = "SELECT * FROM tb_operadoras"
        if filtro:
            query += f" WHERE razao_social ILIKE '%{filtro}%' OR nome_fantasia ILIKE '%{filtro}%'"

        cursor.execute(query)
        colunas = [desc[0] for desc in cursor.description]
        dados = [dict(zip(colunas, row)) for row in cursor.fetchall()]

        cursor.close()
        conn.close()
        return dados
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

