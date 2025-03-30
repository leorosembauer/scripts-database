import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

URL_DEMONSTRACOES = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
URL_OPERADORAS = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "arquivos")
DEMONSTRACOES_DIR = os.path.join(BASE_DIR, "demonstracoes_contabeis")
OPERADORAS_DIR = os.path.join(BASE_DIR, "operadoras_ativas")

os.makedirs(DEMONSTRACOES_DIR, exist_ok=True)
os.makedirs(OPERADORAS_DIR, exist_ok=True)

current_year = datetime.now().year
anos_relevantes = [str(current_year - i) for i in range(3)]

def obter_links_pasta(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        return [urljoin(url, a["href"]) for a in soup.find_all("a", href=True)]
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao acessar {url}: {e}")
        return []

def baixar_arquivo(url, destino):
    if os.path.exists(destino):
        print(f"✔ Arquivo já existe: {os.path.basename(destino)}")
        return

    print(f"⬇ Baixando: {os.path.basename(destino)}")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(destino, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"✅ Download concluído: {os.path.basename(destino)}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao baixar {os.path.basename(destino)}: {e}")

def baixar_demonstracoes_contabeis():
    print(f"🔍 Acessando: {URL_DEMONSTRACOES}")

    for ano in anos_relevantes:
        pasta_ano = urljoin(URL_DEMONSTRACOES, f"{ano}/")
        print(f"📂 Entrando na pasta: {ano}")

        arquivos = obter_links_pasta(pasta_ano)  # Lista os arquivos dentro do ano

        if not arquivos:
            print(f"⚠ Nenhum arquivo encontrado para {ano}. Verifique a URL: {pasta_ano}")
            continue

        print(f"🔍 {len(arquivos)} arquivos encontrados para {ano}")

        for arquivo in arquivos:
            if arquivo.endswith(".zip"):
                nome_arquivo = os.path.basename(arquivo)
                destino_arquivo = os.path.join(DEMONSTRACOES_DIR, nome_arquivo)
                baixar_arquivo(arquivo, destino_arquivo)

def baixar_operadoras_ativas():
    print(f"🔍 Acessando: {URL_OPERADORAS}")
    arquivos = obter_links_pasta(URL_OPERADORAS)

    if not arquivos:
        print(f"⚠ Nenhum arquivo encontrado em {URL_OPERADORAS}")
        return

    for arquivo in arquivos:
        if arquivo.endswith(".csv"):
            nome_arquivo = os.path.basename(arquivo)
            destino_arquivo = os.path.join(OPERADORAS_DIR, nome_arquivo)
            baixar_arquivo(arquivo, destino_arquivo)

baixar_demonstracoes_contabeis()
baixar_operadoras_ativas()