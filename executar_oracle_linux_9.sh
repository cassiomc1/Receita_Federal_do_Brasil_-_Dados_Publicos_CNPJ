#!/usr/bin/env bash
# ==================================================================================================
# Script: executar_oracle_linux_9.sh
# Finalidade: Automação completa para Oracle Linux 9 (ou RHEL 9 / Rocky 9 / AlmaLinux 9):
#   1. Instalação e configuração otimizada do PostgreSQL 16 (com detecção de passos já concluídos)
#   2. Instalação de dependências do sistema e do ambiente Python
#   3. Criação da base de dados "Dados_RFB" e ajuste de permissões
#   4. Garantia de uso da versão moderna do ETL (WebDAV Nextcloud oficial da RFB, sem IP descontinuado)
#   5. Download automático dos dados públicos mais recentes de CNPJ (com suporte a resumo)
#   6. Descompactação inteligente (pula arquivos já extraídos com mesmo tamanho)
#   7. Carga de alta performance com PostgreSQL COPY e retomada por checkpoint (não refaz o que já fez)
#   8. Criação de índices para consultas rápidas
#
# Uso:
#   sudo ./executar_oracle_linux_9.sh [OPÇÕES]
# ==================================================================================================

set -euo pipefail

# --------------------------------------------------------------------------------------------------
# Cores e Formatação de Log
# --------------------------------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCESSO]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[AVISO]${NC} $1"; }
log_error()   { echo -e "${RED}[ERRO]${NC} $1"; }
log_title()   {
    echo -e "\n${CYAN}${BOLD}======================================================================${NC}"
    echo -e "${CYAN}${BOLD} $1 ${NC}"
    echo -e "${CYAN}${BOLD}======================================================================${NC}\n"
}

# --------------------------------------------------------------------------------------------------
# Localização Inteligente do Repositório e Arquivos do Projeto
# --------------------------------------------------------------------------------------------------
SCRIPT_EXEC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIGINAL_USER="${SUDO_USER:-$USER}"
ORIGINAL_HOME="$HOME"
if command -v getent &>/dev/null; then
    ORIGINAL_HOME="$(getent passwd "$ORIGINAL_USER" 2>/dev/null | cut -d: -f6 || echo "$HOME")"
fi

PROJECT_DIR=""
CANDIDATE_DIRS=(
    "$SCRIPT_EXEC_DIR"
    "$SCRIPT_EXEC_DIR/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ"
    "$(pwd)"
    "$(pwd)/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ"
    "${ORIGINAL_HOME}/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ"
    "/home/opc/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ"
)

for dir in "${CANDIDATE_DIRS[@]}"; do
    if [[ -d "$dir/code" && -f "$dir/code/ETL_coletar_dados_e_gravar_BD.py" ]]; then
        PROJECT_DIR="$dir"
        break
    fi
done

if [[ -z "$PROJECT_DIR" ]]; then
    FOUND_CODE=$(find "$SCRIPT_EXEC_DIR" "$ORIGINAL_HOME" "/home/opc" "$(pwd)" -maxdepth 3 -name "ETL_coletar_dados_e_gravar_BD.py" 2>/dev/null | head -1 || true)
    if [[ -n "$FOUND_CODE" ]]; then
        PROJECT_DIR="$(cd "$(dirname "$FOUND_CODE")/.." && pwd)"
    fi
fi

if [[ -z "$PROJECT_DIR" ]]; then
    PROJECT_DIR="${SCRIPT_EXEC_DIR}"
    if [[ ! -d "${PROJECT_DIR}/code" ]]; then
        mkdir -p "${PROJECT_DIR}/code"
    fi
fi

cd "$PROJECT_DIR"
log_info "Diretório ativo do projeto: $PROJECT_DIR"

# --------------------------------------------------------------------------------------------------
# Auto-Reparo e Garantia da Versão Moderna do Código ETL
# --------------------------------------------------------------------------------------------------
# Garante que o arquivo requirements.txt SEMPRE exista no diretório do projeto
if [[ ! -f "${PROJECT_DIR}/requirements.txt" ]]; then
    log_warn "requirements.txt não encontrado em $PROJECT_DIR. Gerando arquivo automaticamente..."
    cat << 'EOF' > "${PROJECT_DIR}/requirements.txt"
pandas>=2.0.0
psycopg2-binary>=2.9.9
SQLAlchemy>=2.0.0
requests>=2.31.0
python-dotenv>=1.0.1
tqdm>=4.66.0
certifi>=2024.2.2
urllib3>=2.0.0
EOF
    log_success "requirements.txt criado com sucesso."
fi

# Garante que code/ETL_coletar_dados_e_gravar_BD.py utilize o novo WebDAV da RFB e não o IP antigo (200.152.38.155)
ensure_modern_etl_script() {
    local target="${PROJECT_DIR}/code/ETL_coletar_dados_e_gravar_BD.py"
    local needs_update=false

    if [[ ! -f "$target" ]]; then
        log_warn "code/ETL_coletar_dados_e_gravar_BD.py não encontrado. Criando versão moderna..."
        needs_update=true
    elif grep -q "200.152.38.155" "$target" 2>/dev/null; then
        log_warn "Detectado IP descontinuado da Receita Federal (200.152.38.155) em $target!"
        log_info "Atualizando automaticamente para o novo servidor WebDAV oficial da Receita Federal..."
        needs_update=true
    elif ! grep -q "webdav_base_url" "$target" 2>/dev/null; then
        log_warn "Versão de $target desatualizada (sem suporte a WebDAV). Atualizando..."
        needs_update=true
    fi

    if [ "$needs_update" = true ]; then
        mkdir -p "${PROJECT_DIR}/code"
        cat << 'EOF_PYTHON_ETL' > "$target"
# -*- coding: utf-8 -*-
"""
Processo de ETL para Coleta e Carga dos Dados Públicos do CNPJ da Receita Federal do Brasil no PostgreSQL.

Atualizado para o novo servidor de Dados Abertos (WebDAV Nextcloud da RFB),
ingestão de alta performance com COPY no PostgreSQL, retomada por checkpoints
e suporte ao CNPJ Alfanumérico (IN RFB nº 2.229/2024).

Autor original: Aphonso Henrique do Amaral Rafael
Atualizado: 2026
"""

#%%
import csv
import gc
import io
import os
import pathlib
import re
import sys
import time
import urllib3
from xml.etree import ElementTree
import zipfile

from dotenv import load_dotenv
import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine
from tqdm import tqdm

# Desabilita avisos de certificados SSL não verificados (comum em servidores gov.br / ICP-Brasil)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#%%
# Funções utilitárias
def makedirs(path):
    """Cria diretório caso não exista."""
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def find_dotenv():
    """Localiza automaticamente o arquivo .env no diretório de execução, do script ou na raiz."""
    candidates = [
        pathlib.Path().resolve() / ".env",
        pathlib.Path(__file__).resolve().parent / ".env",
        pathlib.Path(__file__).resolve().parent.parent / ".env",
    ]
    for p in candidates:
        if p.is_file():
            return str(p)
    return None

#%%
# Configuração de Ambiente
dotenv_path = find_dotenv()
if dotenv_path:
    print(f"Carregando arquivo de configuração: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
else:
    print("Arquivo .env não localizado automaticamente. Tentando variáveis de ambiente do sistema...")
    load_dotenv()

def getEnv(env_var, default=""):
    val = os.getenv(env_var)
    return val if val is not None else default

output_files = getEnv("OUTPUT_FILES_PATH", "./dados/output_files")
extracted_files = getEnv("EXTRACTED_FILES_PATH", "./dados/extracted_files")
makedirs(output_files)
makedirs(extracted_files)

print(
    "Diretórios definidos:\n"
    f"  - output_files: {output_files}\n"
    f"  - extracted_files: {extracted_files}"
)

# Conexão com o Banco de Dados PostgreSQL
db_user = getEnv("DB_USER", "postgres")
db_pass = getEnv("DB_PASSWORD", "postgres")
db_host = getEnv("DB_HOST", "localhost")
db_port = getEnv("DB_PORT", "5432")
db_name = getEnv("DB_NAME", "Dados_RFB")

db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

# Configurações opcionais da Receita Federal
share_token = getEnv("RFB_SHARE_TOKEN", "YggdBLfdninEJX9")
webdav_base_url = getEnv("RFB_WEBDAV_URL", "https://arquivos.receitafederal.gov.br/public.php/webdav")
config_ano_mes = getEnv("RFB_ANO_MES", "")
pular_download = getEnv("PULAR_DOWNLOAD", "False").lower() in ("true", "1", "sim", "s")
pular_extracao = getEnv("PULAR_EXTRACAO", "False").lower() in ("true", "1", "sim", "s")

#%%
# Ingestão de Alta Performance com PostgreSQL COPY
def to_sql_fast(dataframe, name, engine, if_exists="append"):
    """
    Insere dados de um DataFrame no PostgreSQL via comando COPY (alta performance),
    com fallback automático para to_sql convencional em caso de falha.
    """
    if dataframe.empty:
        return

    # Garante que a tabela existe no PostgreSQL (se não existir, cria a estrutura com 0 linhas)
    try:
        dataframe.head(0).to_sql(name=name, con=engine, if_exists=if_exists, index=False)
    except Exception:
        pass

    # Tenta inserção via COPY STDIN direto pelo cursor psycopg2
    try:
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                s_buf = io.StringIO()
                dataframe.to_csv(
                    s_buf,
                    sep=";",
                    index=False,
                    header=False,
                    na_rep="",
                    quoting=csv.QUOTE_MINIMAL,
                    lineterminator="\n",
                )
                s_buf.seek(0)
                columns = ", ".join([f"\"{col}\"" for col in dataframe.columns])
                sql = f"COPY \"{name}\" ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER ';', QUOTE '\"', NULL '')"
                cur.copy_expert(sql=sql, file=s_buf)
            raw_conn.commit()
        finally:
            raw_conn.close()
    except Exception:
        # Fallback para to_sql padrão do SQLAlchemy
        dataframe.to_sql(name=name, con=engine, if_exists="append", index=False, chunksize=4096)

#%%
# Conexão com o banco de dados
print("\nConectando ao banco de dados PostgreSQL...")
try:
    engine = create_engine(db_url)
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        host=db_host,
        port=db_port,
        password=db_pass,
    )
    cur = conn.cursor()
    print("Conexão estabelecida com sucesso!")
except Exception as e:
    print(f"Aviso/Erro na conexão com o banco de dados: {e}")
    print("Verifique se o PostgreSQL está em execução e os dados no arquivo .env.")
    engine = None
    conn = None
    cur = None

#%%
########################################################################################################################
## CONSULTA E DESCOBERTA DE ARQUIVOS (WEBDAV RFB) #####################################################################
########################################################################################################################
def listar_arquivos_rfb(token=share_token, base_url=webdav_base_url, ano_mes_desejado=config_ano_mes):
    """Consulta o servidor WebDAV da Receita Federal e localiza os arquivos mais recentes."""
    DAV_NS = {"d": "DAV:"}
    url = base_url.rstrip("/") + "/"

    headers = {"Depth": "1"}
    response = requests.request("PROPFIND", url, auth=(token, ""), headers=headers, verify=False, timeout=30)
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)

    directories = []
    for resp in root.findall("d:response", DAV_NS):
        href = resp.find("d:href", DAV_NS).text
        match = re.search(r"(\d{4}-\d{2})/?$", href)
        if match:
            directories.append(match.group(1))

    directories.sort()
    if not directories:
        raise RuntimeError("Nenhuma pasta mensal (YYYY-MM) encontrada no servidor.")

    mes_selecionado = ano_mes_desejado if ano_mes_desejado else directories[-1]
    print(f"Mês/Ano de referência da base: {mes_selecionado}")

    url_mes = f"{url}{mes_selecionado}/"
    resp_mes = requests.request("PROPFIND", url_mes, auth=(token, ""), headers=headers, verify=False, timeout=30)
    resp_mes.raise_for_status()
    root_mes = ElementTree.fromstring(resp_mes.content)

    files = []
    for resp in root_mes.findall("d:response", DAV_NS):
        href = resp.find("d:href", DAV_NS).text
        match = re.search(r"/([^/]+\.zip)$", href, re.IGNORECASE)
        if match:
            files.append(match.group(1))

    files.sort()
    download_base_url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{token}/{mes_selecionado}/"
    return mes_selecionado, download_base_url, files

#%%
# Obter lista de arquivos
if not pular_download:
    print("\nConsultando lista de arquivos na Receita Federal...")
    mes_referencia, base_download_url, files_to_download = listar_arquivos_rfb()
    print(f"Total de arquivos encontrados ({len(files_to_download)}):")
    for idx, f in enumerate(files_to_download, 1):
        print(f"  {idx:02d} - {f}")
else:
    print("\nPULAR_DOWNLOAD=True: Etapa de consulta e download ignorada.")
    files_to_download = []
    base_download_url = ""

#%%
########################################################################################################################
## DOWNLOAD COM RESUMO E BARRA DE PROGRESSO ###########################################################################
########################################################################################################################
def download_arquivo(url, destino, chunk_size=1024 * 1024):
    """Baixa um arquivo com suporte a resumo (resume) e exibição de barra de progresso."""
    session = requests.Session()
    try:
        resp_head = session.head(url, verify=False, timeout=30)
        total_size = int(resp_head.headers.get("content-length", 0))
    except Exception:
        total_size = 0

    resume_byte = 0
    if os.path.isfile(destino):
        local_size = os.path.getsize(destino)
        if total_size > 0 and local_size == total_size:
            print(f"Arquivo já baixado e íntegro: {os.path.basename(destino)}")
            return
        elif total_size > 0 and local_size > total_size:
            os.remove(destino)
            resume_byte = 0
        else:
            resume_byte = local_size

    headers = {}
    mode = "wb"
    if resume_byte > 0:
        headers["Range"] = f"bytes={resume_byte}-"
        mode = "ab"

    with session.get(url, headers=headers, stream=True, verify=False, timeout=60) as r:
        if resume_byte > 0 and r.status_code == 416:
            return
        r.raise_for_status()
        with open(destino, mode) as f, tqdm(
            total=total_size,
            initial=resume_byte,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=os.path.basename(destino),
        ) as pbar:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

if not pular_download:
    print("\nIniciando download dos arquivos da Receita Federal...")
    for idx, nome_arquivo in enumerate(files_to_download, 1):
        url_arq = base_download_url + nome_arquivo
        caminho_local = os.path.join(output_files, nome_arquivo)
        print(f"\n[{idx}/{len(files_to_download)}] Baixando arquivo: {nome_arquivo}")
        download_arquivo(url_arq, caminho_local)

    # Download do layout / metadados atualizado oficial
    print("\nBaixando layout de metadados oficial (PDF)...")
    try:
        url_layout = "https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"
        caminho_layout = os.path.join(output_files, "cnpj-metadados.pdf")
        download_arquivo(url_layout, caminho_layout)
    except Exception as e:
        print(f"Aviso: não foi possível baixar o layout PDF: {e}")

#%%
########################################################################################################################
## EXTRAÇÃO DOS ARQUIVOS ZIP ###########################################################################################
########################################################################################################################
if not pular_extracao:
    print("\nIniciando descompactação dos arquivos...")
    zips_no_diretorio = [f for f in os.listdir(output_files) if f.lower().endswith(".zip")]
    zips_no_diretorio.sort()
    for idx, nome_zip in enumerate(zips_no_diretorio, 1):
        caminho_zip = os.path.join(output_files, nome_zip)
        print(f"[{idx}/{len(zips_no_diretorio)}] Descompactando: {nome_zip} [...]", end="", flush=True)
        try:
            with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
                # Otimização: pula arquivos que já foram descompactados com mesmo tamanho
                ja_extraido = True
                for file_info in zip_ref.infolist():
                    extracted_path = os.path.join(extracted_files, file_info.filename)
                    if not os.path.isfile(extracted_path) or os.path.getsize(extracted_path) != file_info.file_size:
                        ja_extraido = False
                        break
                if ja_extraido:
                    print(" Já descompactado previamente (pulando).")
                    continue

                zip_ref.extractall(extracted_files)
            print(" Concluído!")
        except Exception as e:
            print(f" Erro: {e}")
else:
    print("\nPULAR_EXTRACAO=True: Etapa de descompactação ignorada.")

#%%
########################################################################################################################
## IDENTIFICAÇÃO DOS ARQUIVOS DESCOMPACTADOS ############################################################################
########################################################################################################################
itens_extraidos = [
    f for f in os.listdir(extracted_files)
    if os.path.isfile(os.path.join(extracted_files, f)) and not f.lower().endswith(".zip")
]

arquivos_empresa = sorted([f for f in itens_extraidos if "EMPRE" in f.upper() and "ESTABELE" not in f.upper()])
arquivos_estabelecimento = sorted([f for f in itens_extraidos if "ESTABELE" in f.upper()])
arquivos_socios = sorted([f for f in itens_extraidos if "SOCIO" in f.upper()])
arquivos_simples = sorted([f for f in itens_extraidos if "SIMPLES" in f.upper()])
arquivos_cnae = sorted([f for f in itens_extraidos if "CNAE" in f.upper()])
arquivos_moti = sorted([f for f in itens_extraidos if "MOTI" in f.upper()])
arquivos_munic = sorted([f for f in itens_extraidos if "MUNIC" in f.upper()])
arquivos_natju = sorted([f for f in itens_extraidos if "NATJU" in f.upper()])
arquivos_pais = sorted([f for f in itens_extraidos if "PAIS" in f.upper()])
arquivos_quals = sorted([f for f in itens_extraidos if "QUALS" in f.upper()])

print("\nArquivos identificados para carga no banco de dados:")
print(f"  - Empresas: {len(arquivos_empresa)}")
print(f"  - Estabelecimentos: {len(arquivos_estabelecimento)}")
print(f"  - Sócios: {len(arquivos_socios)}")
print(f"  - Simples Nacional: {len(arquivos_simples)}")
print(f"  - CNAE: {len(arquivos_cnae)}")
print(f"  - Motivos: {len(arquivos_moti)}")
print(f"  - Municípios: {len(arquivos_munic)}")
print(f"  - Natureza Jurídica: {len(arquivos_natju)}")
print(f"  - Países: {len(arquivos_pais)}")
print(f"  - Qualificação de Sócios: {len(arquivos_quals)}")

if engine is None or conn is None:
    print("\n[AVISO CRÍTICO] Banco de dados não conectado. Interrompendo processo de carga.")
    sys.exit(1)

#%%
########################################################################################################################
## CONTROLE DE ESTADO E RETOMADA (CHECKPOINTING) ######################################################################
########################################################################################################################
reset_etl = getEnv("RESET_ETL", "False").lower() in ("true", "1", "sim", "s")
if reset_etl:
    print("\n[AVISO] RESET_ETL=True detectado: limpando histórico de controle e recarregando do zero.")
    cur.execute('DROP TABLE IF EXISTS "_controle_etl";')
    conn.commit()

cur.execute("""
CREATE TABLE IF NOT EXISTS "_controle_etl" (
    "arquivo" VARCHAR(255) PRIMARY KEY,
    "etapa" VARCHAR(100),
    "status" VARCHAR(50),
    "concluido_em" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
conn.commit()

def arquivo_ja_processado(nome_arquivo):
    """Verifica se determinado arquivo já foi 100% carregado no banco."""
    try:
        cur.execute('SELECT 1 FROM "_controle_etl" WHERE "arquivo" = %s AND "status" = %s;', (nome_arquivo, "CONCLUIDO"))
        return cur.fetchone() is not None
    except Exception:
        conn.rollback()
        return False

def registrar_arquivo_processado(etapa, nome_arquivo):
    """Registra que o arquivo foi totalmente carregado no banco."""
    try:
        cur.execute(
            'INSERT INTO "_controle_etl" ("arquivo", "etapa", "status", "concluido_em") '
            'VALUES (%s, %s, %s, NOW()) '
            'ON CONFLICT ("arquivo") DO UPDATE SET "status" = %s, "concluido_em" = NOW();',
            (nome_arquivo, etapa, "CONCLUIDO", "CONCLUIDO")
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Aviso ao registrar progresso de {nome_arquivo}: {e}")

#%%
########################################################################################################################
## CARGA: EMPRESAS ####################################################################################################
########################################################################################################################
CHUNK_SIZE = 100000

if arquivos_empresa:
    print("\n" + "=" * 60)
    print("CARGA: EMPRESA")
    print("=" * 60)

    arquivos_pendentes = [f for f in arquivos_empresa if not arquivo_ja_processado(f)]
    if not arquivos_pendentes:
        print(f"Todos os {len(arquivos_empresa)} arquivos de EMPRESA já foram carregados previamente no banco. Pulando etapa!")
    else:
        arquivos_concluidos = [f for f in arquivos_empresa if arquivo_ja_processado(f)]
        if not arquivos_concluidos:
            cur.execute('DROP TABLE IF EXISTS "empresa";')
            conn.commit()
        else:
            print(f"{len(arquivos_concluidos)} de {len(arquivos_empresa)} arquivos já carregados. Retomando carga dos {len(arquivos_pendentes)} arquivos pendentes...")

        t0 = time.time()
        empresa_dtypes = {0: object, 1: object, 2: "Int32", 3: "Int32", 4: object, 5: "Int32", 6: object}
        colunas_empresa = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel",
        ]

        for arq in arquivos_pendentes:
            print(f"Processando arquivo: {arq} [...]")
            caminho = os.path.join(extracted_files, arq)
            for chunk in pd.read_csv(
                caminho,
                sep=";",
                header=None,
                dtype=empresa_dtypes,
                encoding="latin-1",
                chunksize=CHUNK_SIZE,
            ):
                chunk.columns = colunas_empresa
                chunk["capital_social"] = (
                    chunk["capital_social"].astype(str).str.replace(",", ".", regex=False).astype(float)
                )
                to_sql_fast(chunk, name="empresa", engine=engine)
            registrar_arquivo_processado("empresa", arq)
            print(f"Arquivo {arq} inserido e registrado com sucesso!")

        print(f"Tempo de carga de Empresas: {round(time.time() - t0)} segundos.")
        gc.collect()

#%%
########################################################################################################################
## CARGA: ESTABELECIMENTOS ############################################################################################
########################################################################################################################
if arquivos_estabelecimento:
    print("\n" + "=" * 60)
    print("CARGA: ESTABELECIMENTO")
    print("=" * 60)

    arquivos_pendentes = [f for f in arquivos_estabelecimento if not arquivo_ja_processado(f)]
    if not arquivos_pendentes:
        print(f"Todos os {len(arquivos_estabelecimento)} arquivos de ESTABELECIMENTO já foram carregados previamente no banco. Pulando etapa!")
    else:
        arquivos_concluidos = [f for f in arquivos_estabelecimento if arquivo_ja_processado(f)]
        if not arquivos_concluidos:
            cur.execute('DROP TABLE IF EXISTS "estabelecimento";')
            conn.commit()
        else:
            print(f"{len(arquivos_concluidos)} de {len(arquivos_estabelecimento)} arquivos já carregados. Retomando carga dos {len(arquivos_pendentes)} arquivos pendentes...")

        t0 = time.time()
        estabelecimento_dtypes = {
            0: object, 1: object, 2: object, 3: "Int32", 4: object, 5: "Int32", 6: "Int32",
            7: "Int32", 8: object, 9: object, 10: "Int32", 11: "Int32", 12: object,
            13: object, 14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
            20: "Int32", 21: object, 22: object, 23: object, 24: object, 25: object,
            26: object, 27: object, 28: object, 29: "Int32",
        }
        colunas_estabelecimento = [
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "identificador_matriz_filial",
            "nome_fantasia",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "nome_cidade_exterior",
            "pais",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "cnae_fiscal_secundaria",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "municipio",
            "ddd_1",
            "telefone_1",
            "ddd_2",
            "telefone_2",
            "ddd_fax",
            "fax",
            "correio_eletronico",
            "situacao_especial",
            "data_situacao_especial",
        ]

        for arq in arquivos_pendentes:
            print(f"Processando arquivo: {arq} [...]")
            caminho = os.path.join(extracted_files, arq)
            for chunk in pd.read_csv(
                caminho,
                sep=";",
                header=None,
                dtype=estabelecimento_dtypes,
                encoding="latin-1",
                chunksize=CHUNK_SIZE,
            ):
                chunk.columns = colunas_estabelecimento
                to_sql_fast(chunk, name="estabelecimento", engine=engine)
            registrar_arquivo_processado("estabelecimento", arq)
            print(f"Arquivo {arq} inserido e registrado com sucesso!")

        print(f"Tempo de carga de Estabelecimentos: {round(time.time() - t0)} segundos.")
        gc.collect()

#%%
########################################################################################################################
## CARGA: SÓCIOS #######################################################################################################
########################################################################################################################
if arquivos_socios:
    print("\n" + "=" * 60)
    print("CARGA: SÓCIOS")
    print("=" * 60)

    arquivos_pendentes = [f for f in arquivos_socios if not arquivo_ja_processado(f)]
    if not arquivos_pendentes:
        print(f"Todos os {len(arquivos_socios)} arquivos de SÓCIOS já foram carregados previamente no banco. Pulando etapa!")
    else:
        arquivos_concluidos = [f for f in arquivos_socios if arquivo_ja_processado(f)]
        if not arquivos_concluidos:
            cur.execute('DROP TABLE IF EXISTS "socios";')
            conn.commit()
        else:
            print(f"{len(arquivos_concluidos)} de {len(arquivos_socios)} arquivos já carregados. Retomando carga dos {len(arquivos_pendentes)} arquivos pendentes...")

        t0 = time.time()
        socios_dtypes = {
            0: object, 1: "Int32", 2: object, 3: object, 4: "Int32", 5: "Int32", 6: "Int32",
            7: object, 8: object, 9: "Int32", 10: "Int32",
        }
        colunas_socios = [
            "cnpj_basico",
            "identificador_socio",
            "nome_socio_razao_social",
            "cpf_cnpj_socio",
            "qualificacao_socio",
            "data_entrada_sociedade",
            "pais",
            "representante_legal",
            "nome_do_representante",
            "qualificacao_representante_legal",
            "faixa_etaria",
        ]

        for arq in arquivos_pendentes:
            print(f"Processando arquivo: {arq} [...]")
            caminho = os.path.join(extracted_files, arq)
            for chunk in pd.read_csv(
                caminho,
                sep=";",
                header=None,
                dtype=socios_dtypes,
                encoding="latin-1",
                chunksize=CHUNK_SIZE,
            ):
                chunk.columns = colunas_socios
                to_sql_fast(chunk, name="socios", engine=engine)
            registrar_arquivo_processado("socios", arq)
            print(f"Arquivo {arq} inserido e registrado com sucesso!")

        print(f"Tempo de carga de Sócios: {round(time.time() - t0)} segundos.")
        gc.collect()

#%%
########################################################################################################################
## CARGA: SIMPLES NACIONAL #############################################################################################
########################################################################################################################
if arquivos_simples:
    print("\n" + "=" * 60)
    print("CARGA: SIMPLES NACIONAL / MEI")
    print("=" * 60)

    arquivos_pendentes = [f for f in arquivos_simples if not arquivo_ja_processado(f)]
    if not arquivos_pendentes:
        print(f"Todos os {len(arquivos_simples)} arquivos de SIMPLES NACIONAL já foram carregados previamente no banco. Pulando etapa!")
    else:
        arquivos_concluidos = [f for f in arquivos_simples if arquivo_ja_processado(f)]
        if not arquivos_concluidos:
            cur.execute('DROP TABLE IF EXISTS "simples";')
            conn.commit()
        else:
            print(f"{len(arquivos_concluidos)} de {len(arquivos_simples)} arquivos já carregados. Retomando carga...")

        t0 = time.time()
        simples_dtypes = {0: object, 1: object, 2: "Int32", 3: "Int32", 4: object, 5: "Int32", 6: "Int32"}
        colunas_simples = [
            "cnpj_basico",
            "opcao_pelo_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_mei",
            "data_opcao_mei",
            "data_exclusao_mei",
        ]

        for arq in arquivos_pendentes:
            print(f"Processando arquivo: {arq} [...]")
            caminho = os.path.join(extracted_files, arq)
            for chunk in pd.read_csv(
                caminho,
                sep=";",
                header=None,
                dtype=simples_dtypes,
                encoding="latin-1",
                chunksize=CHUNK_SIZE,
            ):
                chunk.columns = colunas_simples
                to_sql_fast(chunk, name="simples", engine=engine)
            registrar_arquivo_processado("simples", arq)
            print(f"Arquivo {arq} inserido e registrado com sucesso!")

        print(f"Tempo de carga do Simples Nacional: {round(time.time() - t0)} segundos.")
        gc.collect()

#%%
########################################################################################################################
## CARGA: TABELAS DE DOMÍNIO ###########################################################################################
########################################################################################################################
def carregar_tabela_dominio(nome_tabela, arquivos, dtypes="object", colunas=["codigo", "descricao"]):
    if not arquivos:
        return

    arquivos_pendentes = [f for f in arquivos if not arquivo_ja_processado(f)]
    if not arquivos_pendentes:
        print(f"Domínio {nome_tabela.upper()} já carregado previamente no banco. Pulando!")
        return

    print(f"\nProcessando domínio: {nome_tabela.upper()} [...]")
    t0 = time.time()
    cur.execute(f'DROP TABLE IF EXISTS "{nome_tabela}";')
    conn.commit()

    for arq in arquivos_pendentes:
        caminho = os.path.join(extracted_files, arq)
        df = pd.read_csv(caminho, sep=";", header=None, dtype=dtypes, encoding="latin-1")
        df.columns = colunas
        to_sql_fast(df, name=nome_tabela, engine=engine)
        registrar_arquivo_processado(nome_tabela, arq)
        print(f"Arquivo {arq} inserido na tabela {nome_tabela}!")

    print(f"Tempo de carga de {nome_tabela}: {round(time.time() - t0, 2)} segundos.")

carregar_tabela_dominio("cnae", arquivos_cnae, dtypes="object")
carregar_tabela_dominio("moti", arquivos_moti, dtypes={0: "Int32", 1: object})
carregar_tabela_dominio("munic", arquivos_munic, dtypes={0: "Int32", 1: object})
carregar_tabela_dominio("natju", arquivos_natju, dtypes={0: "Int32", 1: object})
carregar_tabela_dominio("pais", arquivos_pais, dtypes={0: "Int32", 1: object})
carregar_tabela_dominio("quals", arquivos_quals, dtypes={0: "Int32", 1: object})

#%%
########################################################################################################################
## ÍNDICES NO BANCO DE DADOS ###########################################################################################
########################################################################################################################
if arquivo_ja_processado("INDICES_CRIADOS"):
    print("\nÍndices de alta performance já foram criados previamente. Pulando criação de índices.")
else:
    print("\n" + "=" * 60)
    print("Criando índices de alta performance nas tabelas...")
    print("=" * 60)
    t_idx = time.time()

    indices_sql = """
    CREATE INDEX IF NOT EXISTS empresa_cnpj ON "empresa"("cnpj_basico");
    CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON "estabelecimento"("cnpj_basico");
    CREATE INDEX IF NOT EXISTS socios_cnpj ON "socios"("cnpj_basico");
    CREATE INDEX IF NOT EXISTS simples_cnpj ON "simples"("cnpj_basico");
    """
    cur.execute(indices_sql)
    conn.commit()
    registrar_arquivo_processado("indices", "INDICES_CRIADOS")

    print(f"Índices criados com sucesso em {round(time.time() - t_idx, 2)} segundos!")

print("""
Tabelas indexadas por cnpj_basico:
  - empresa
  - estabelecimento
  - socios
  - simples
""")

print("\nProcesso de ETL 100% finalizado! Os dados estão prontos para consulta no PostgreSQL.")
EOF_PYTHON_ETL
        log_success "code/ETL_coletar_dados_e_gravar_BD.py atualizado para a versão moderna oficial com sucesso!"
    fi
}

ensure_modern_etl_script

# --------------------------------------------------------------------------------------------------
# Variáveis de Configuração Padrão
# --------------------------------------------------------------------------------------------------
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_NAME="Dados_RFB"

DATA_DIR="${PROJECT_DIR}/dados"
OUTPUT_PATH="${DATA_DIR}/output_files"
EXTRACTED_PATH="${DATA_DIR}/extracted_files"

SKIP_INSTALL=false
SKIP_DOWNLOAD=false
SKIP_EXTRACT=false
ASSUME_YES=false
RESET_ETL=false
RFB_ANO_MES=""

# --------------------------------------------------------------------------------------------------
# Exibição de Ajuda
# --------------------------------------------------------------------------------------------------
show_help() {
    cat << EOF
Uso: sudo $0 [OPÇÕES]

Opções:
  -p, --db-password SENHA   Senha para o usuário postgres (padrão: 'postgres')
  -d, --db-name NOME        Nome da base de dados (padrão: 'Dados_RFB')
  --port PORTA              Porta do PostgreSQL (padrão: 5432)
  --data-dir DIRETORIO      Diretório raiz para armazenar os dados (padrão: '${PROJECT_DIR}/dados')
  -m, --mes YYYY-MM         Mês/ano de referência fixo (ex: 2026-08). Padrão: mais recente disponível
  -y, --yes                 Responde 'sim' para todas as confirmações
  --reset                   Limpa histórico de controle do ETL e recarrega tabelas do zero
  --skip-install            Pular instalação do PostgreSQL e pacotes do sistema
  --skip-download           Pular download dos arquivos (caso já tenham sido baixados)
  --skip-extract            Pular descompactação (caso já tenham sido descompactados)
  -h, --help                Exibe esta tela de ajuda

Exemplo de uso:
  sudo ./executar_oracle_linux_9.sh

Exemplo customizado:
  sudo ./executar_oracle_linux_9.sh --db-password "minhasenha123" --data-dir "/dados/rfb" -y
EOF
    exit 0
}

# --------------------------------------------------------------------------------------------------
# Tratamento de Parâmetros
# --------------------------------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--db-password)
            DB_PASSWORD="$2"
            shift 2
            ;;
        -d|--db-name)
            DB_NAME="$2"
            shift 2
            ;;
        --port)
            DB_PORT="$2"
            shift 2
            ;;
        --data-dir)
            DATA_DIR="$2"
            OUTPUT_PATH="${DATA_DIR}/output_files"
            EXTRACTED_PATH="${DATA_DIR}/extracted_files"
            shift 2
            ;;
        -m|--mes)
            RFB_ANO_MES="$2"
            shift 2
            ;;
        -y|--yes)
            ASSUME_YES=true
            shift
            ;;
        --reset)
            RESET_ETL=true
            shift
            ;;
        --skip-install)
            SKIP_INSTALL=true
            shift
            ;;
        --skip-download)
            SKIP_DOWNLOAD=true
            shift
            ;;
        --skip-extract)
            SKIP_EXTRACT=true
            shift
            ;;
        -h|--help)
            show_help
            ;;
        *)
            log_error "Opção desconhecida: $1"
            show_help
            ;;
    esac
done

# --------------------------------------------------------------------------------------------------
# Validações Iniciais de Ambiente
# --------------------------------------------------------------------------------------------------
log_title "INICIANDO PROCESSO - DADOS PÚBLICOS CNPJ (ORACLE LINUX 9)"

if [[ $EUID -ne 0 ]]; then
    log_error "Este script precisa ser executado como root ou com privilégios de sudo para gerenciar pacotes e serviços."
    log_info "Execute com: sudo $0"
    exit 1
fi

# Dica de sessão persistente para SSH
if [[ -z "${TMUX:-}" && -z "${STY:-}" ]]; then
    log_info "Dica para SSH: Como o processo completo pode demorar, execute no tmux para evitar cancelamento:"
    log_info "  tmux new -s rfb"
    log_info "  sudo $0"
    echo ""
fi

# Verificação do SO
if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    OS_ID="${ID:-unknown}"
    OS_VERSION_ID="${VERSION_ID:-0}"
    MAJOR_VER="${OS_VERSION_ID%%.*}"
    log_info "Sistema detectado: $PRETTY_NAME ($OS_ID versão $OS_VERSION_ID)"
    if [[ "$MAJOR_VER" != "9" ]]; then
        log_warn "Aviso: O script é otimizado para a versão 9 da família EL. Versão atual: $MAJOR_VER."
    fi
fi

# Verificação de Espaço em Disco
mkdir -p "$DATA_DIR" "$OUTPUT_PATH" "$EXTRACTED_PATH"
DISK_FREE_KB=$(df -k "$DATA_DIR" 2>/dev/null | tail -1 | awk '{print $(NF-2)}' || echo "104857600")
DISK_FREE_GB=$((DISK_FREE_KB / 1024 / 1024))

log_info "Espaço livre na partição de dados ($DATA_DIR): ${DISK_FREE_GB} GB"
if [[ $DISK_FREE_GB -lt 80 && "$ASSUME_YES" = false ]]; then
    log_warn "ATENÇÃO: Recomenda-se pelo menos 80GB a 100GB livres. Você possui ${DISK_FREE_GB} GB."
    if [ -t 0 ]; then
        echo -n "Deseja continuar mesmo assim? [s/N]: "
        read -r resp
        if [[ ! "$resp" =~ ^[sSyY]$ ]]; then
            log_info "Operação cancelada pelo usuário."
            exit 0
        fi
    else
        log_warn "Execução não interativa detectada: prosseguindo com a operação..."
    fi
fi

# Cálculo dinâmico de performance para o PostgreSQL baseado na memória RAM
TOTAL_RAM_KB=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo "4194304")
TOTAL_RAM_MB=$((TOTAL_RAM_KB / 1024))
log_info "Memória RAM Total detectada: ${TOTAL_RAM_MB} MB"

SHARED_BUFFERS_MB=$((TOTAL_RAM_MB / 4))
[[ $SHARED_BUFFERS_MB -gt 4096 ]] && SHARED_BUFFERS_MB=4096
[[ $SHARED_BUFFERS_MB -lt 512 ]] && SHARED_BUFFERS_MB=512

MAINT_WORK_MEM_MB=$((TOTAL_RAM_MB / 8))
[[ $MAINT_WORK_MEM_MB -gt 2048 ]] && MAINT_WORK_MEM_MB=2048
[[ $MAINT_WORK_MEM_MB -lt 256 ]] && MAINT_WORK_MEM_MB=256

# --------------------------------------------------------------------------------------------------
# ETAPA 1: Instalação de Pacotes do Sistema e PostgreSQL 16 (Idempotente)
# --------------------------------------------------------------------------------------------------
log_title "ETAPA 1: VERIFICANDO PACOTES DO SISTEMA E POSTGRESQL 16"

PACKAGES_ALREADY_INSTALLED=false
if (rpm -q postgresql16-server &>/dev/null || rpm -q postgresql-server &>/dev/null) && \
   command -v git &>/dev/null && command -v gcc &>/dev/null && command -v python3 &>/dev/null; then
    PACKAGES_ALREADY_INSTALLED=true
    log_info "Pacotes do sistema e PostgreSQL já detectados como instalados. Pulando DNF..."
fi

if [ "$SKIP_INSTALL" = false ] && [ "$PACKAGES_ALREADY_INSTALLED" = false ]; then
    log_info "Configurando repositório EPEL..."
    dnf install -y oracle-epel-release-el9 || dnf install -y epel-release || true

    log_info "Instalando ferramentas essenciais..."
    dnf install -y git curl wget unzip tar gcc make util-linux tmux procps-ng libpq-devel

    log_info "Instalando Python..."
    if dnf list python3.11 &>/dev/null; then
        dnf install -y python3.11 python3.11-pip python3.11-devel
    else
        dnf install -y python3 python3-pip python3-devel
    fi

    log_info "Configurando repositório oficial PostgreSQL PGDG 16..."
    ARCH="$(uname -m)"
    PGDG_RPM="https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-${ARCH}/pgdg-redhat-repo-latest.noarch.rpm"
    dnf install -y "$PGDG_RPM" || true
    dnf -qy module disable postgresql || true
    dnf install -y postgresql16-server postgresql16-contrib postgresql16
fi

# Configura PATH do PostgreSQL
if [[ -d "/usr/pgsql-16/bin" ]]; then
    export PATH=/usr/pgsql-16/bin:$PATH
    echo 'export PATH=/usr/pgsql-16/bin:$PATH' > /etc/profile.d/pgsql16.sh
fi

# Inicialização do Cluster PostgreSQL (apenas se não inicializado)
PG_DATA_DIR="/var/lib/pgsql/16/data"
[[ ! -d "$PG_DATA_DIR" && -d "/var/lib/pgsql/data" ]] && PG_DATA_DIR="/var/lib/pgsql/data"

if [[ ! -f "${PG_DATA_DIR}/PG_VERSION" ]]; then
    log_info "Inicializando banco de dados do PostgreSQL 16..."
    if [[ -f "/usr/pgsql-16/bin/postgresql-16-setup" ]]; then
        /usr/pgsql-16/bin/postgresql-16-setup initdb
    else
        postgresql-setup --initdb
    fi
else
    log_info "Cluster PostgreSQL já se encontrava inicializado em: $PG_DATA_DIR"
fi

# Configuração de Autenticação no pg_hba.conf (apenas se ainda não ajustado)
PG_HBA="${PG_DATA_DIR}/pg_hba.conf"
if [[ -f "$PG_HBA" ]]; then
    if ! grep -q "# Conexao local por socket Unix - RFB" "$PG_HBA"; then
        log_info "Ajustando regras de autenticação local no pg_hba.conf..."
        cp "$PG_HBA" "${PG_HBA}.backup_$(date +%Y%m%d%H%M%S)"
        cat << 'EOF' > "$PG_HBA"
# Conexao local por socket Unix - RFB
local   all             postgres                                trust
local   all             all                                     peer
# Conexoes locais IPv4 e IPv6
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ::1/128                 scram-sha-256
EOF
    fi
fi

# Garante que o serviço está rodando
if ! systemctl is-active --quiet postgresql-16 2>/dev/null && ! systemctl is-active --quiet postgresql 2>/dev/null; then
    log_info "Iniciando serviço PostgreSQL..."
    systemctl enable postgresql-16 2>/dev/null || systemctl enable postgresql 2>/dev/null || true
    systemctl start postgresql-16 2>/dev/null || systemctl start postgresql 2>/dev/null || true
else
    log_info "Serviço PostgreSQL já está ativo e em execução."
fi

# Localiza psql
PSQL_BIN="psql"
if command -v /usr/pgsql-16/bin/psql &>/dev/null; then
    PSQL_BIN="/usr/pgsql-16/bin/psql"
fi

# Aguarda serviço aceitar conexões
for i in {1..30}; do
    if sudo -u postgres "$PSQL_BIN" -c "SELECT 1;" &>/dev/null; then
        break
    fi
    sleep 1
done

# --------------------------------------------------------------------------------------------------
# ETAPA 2: Configuração e Banco de Dados (Idempotente)
# --------------------------------------------------------------------------------------------------
log_title "ETAPA 2: AJUSTE DE PERFORMANCE E BASE DE DADOS"

CURRENT_SYNCHRONOUS_COMMIT=$(sudo -u postgres "$PSQL_BIN" -tAc "SHOW synchronous_commit;" 2>/dev/null || echo "")
if [[ "$CURRENT_SYNCHRONOUS_COMMIT" != "off" ]]; then
    log_info "Aplicando configurações de alta performance no PostgreSQL..."
    sudo -u postgres "$PSQL_BIN" << EOF
ALTER SYSTEM SET synchronous_commit = 'off';
ALTER SYSTEM SET checkpoint_timeout = '30min';
ALTER SYSTEM SET checkpoint_completion_target = '0.9';
ALTER SYSTEM SET max_wal_size = '16GB';
ALTER SYSTEM SET min_wal_size = '2GB';
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET work_mem = '64MB';
ALTER SYSTEM SET maintenance_work_mem = '${MAINT_WORK_MEM_MB}MB';
ALTER SYSTEM SET shared_buffers = '${SHARED_BUFFERS_MB}MB';
SELECT pg_reload_conf();
EOF
    systemctl restart postgresql-16 2>/dev/null || systemctl restart postgresql 2>/dev/null || true
    for i in {1..30}; do
        sudo -u postgres "$PSQL_BIN" -c "SELECT 1;" &>/dev/null && break
        sleep 1
    done
else
    log_info "Parâmetros de performance do PostgreSQL já configurados previamente."
fi

sudo -u postgres "$PSQL_BIN" -c "ALTER USER postgres WITH PASSWORD '${DB_PASSWORD}';"

DB_EXISTS=$(sudo -u postgres "$PSQL_BIN" -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}';" 2>/dev/null || echo "0")
if [[ "$DB_EXISTS" != "1" ]]; then
    log_info "Criando base de dados '${DB_NAME}'..."
    sudo -u postgres "$PSQL_BIN" -c "CREATE DATABASE \"${DB_NAME}\" OWNER postgres ENCODING 'UTF8';"
    log_success "Base de dados '${DB_NAME}' criada com sucesso!"
else
    log_info "Base de dados '${DB_NAME}' já existe. Pulando criação."
fi

sudo -u postgres "$PSQL_BIN" -c "GRANT ALL PRIVILEGES ON DATABASE \"${DB_NAME}\" TO postgres;"

# --------------------------------------------------------------------------------------------------
# ETAPA 3: Preparação do Ambiente Virtual Python (Idempotente)
# --------------------------------------------------------------------------------------------------
log_title "ETAPA 3: PREPARANDO AMBIENTE VIRTUAL PYTHON"

PYTHON_CMD="python3"
if command -v python3.11 &>/dev/null; then
    PYTHON_CMD="python3.11"
fi

VENV_DIR="${PROJECT_DIR}/venv"
if [[ ! -d "$VENV_DIR" ]]; then
    log_info "Criando ambiente virtual Python em: $VENV_DIR"
    $PYTHON_CMD -m venv "$VENV_DIR"
else
    log_info "Ambiente virtual já existente em: $VENV_DIR"
fi

DEPENDENCIES_READY=false
if "$VENV_DIR/bin/python" -c "import pandas, psycopg2, sqlalchemy, requests, dotenv, tqdm" &>/dev/null; then
    DEPENDENCIES_READY=true
    log_info "Todas as dependências Python já estão instaladas e validadas! Pulando pip install..."
fi

if [ "$DEPENDENCIES_READY" = false ]; then
    log_info "Instalando/atualizando bibliotecas do requirements.txt..."
    "$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel --quiet
    "$VENV_DIR/bin/pip" install -r "${PROJECT_DIR}/requirements.txt" --quiet
    log_success "Dependências instaladas com sucesso!"
fi

# --------------------------------------------------------------------------------------------------
# ETAPA 4: Configuração do Arquivo de Ambiente (.env)
# --------------------------------------------------------------------------------------------------
log_title "ETAPA 4: CONFIGURANDO ARQUIVO DE AMBIENTE (.env)"

mkdir -p "$OUTPUT_PATH" "$EXTRACTED_PATH"

if [[ -n "${ORIGINAL_USER:-}" && "$ORIGINAL_USER" != "root" ]]; then
    chown -R "${ORIGINAL_USER}:${ORIGINAL_USER}" "$DATA_DIR" "$VENV_DIR" 2>/dev/null || true
fi

ENV_CONTENT=$(cat << EOF
# Gerado por executar_oracle_linux_9.sh em $(date)
OUTPUT_FILES_PATH=${OUTPUT_PATH}
EXTRACTED_FILES_PATH=${EXTRACTED_PATH}

# Conexao com o PostgreSQL
DB_HOST=${DB_HOST}
DB_PORT=${DB_PORT}
DB_USER=${DB_USER}
DB_PASSWORD=${DB_PASSWORD}
DB_NAME=${DB_NAME}

# Configuracoes do ETL
RFB_ANO_MES=${RFB_ANO_MES}
PULAR_DOWNLOAD=${SKIP_DOWNLOAD}
PULAR_EXTRACAO=${SKIP_EXTRACT}
RESET_ETL=${RESET_ETL}
EOF
)

echo "$ENV_CONTENT" > "${PROJECT_DIR}/.env"
echo "$ENV_CONTENT" > "${PROJECT_DIR}/code/.env"
log_success "Arquivo .env configurado."

# --------------------------------------------------------------------------------------------------
# ETAPA 5: Execução do Pipeline de ETL (com retomada inteligente)
# --------------------------------------------------------------------------------------------------
log_title "ETAPA 5: EXECUTANDO O PROCESSO DE ETL"

LOG_FILE="${PROJECT_DIR}/etl_rfb_$(date +%Y%m%d_%H%M%S).log"
log_info "Log detalhado sendo gravado em: $LOG_FILE"
log_info "Iniciando processamento Python..."

START_TIME=$(date +%s)
export PYTHONUNBUFFERED=1

"${VENV_DIR}/bin/python" "${PROJECT_DIR}/code/ETL_coletar_dados_e_gravar_BD.py" 2>&1 | tee -a "$LOG_FILE"

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))
HOURS=$((TOTAL_DURATION / 3600))
MINUTES=$(((TOTAL_DURATION % 3600) / 60))
SECONDS=$((TOTAL_DURATION % 60))

# --------------------------------------------------------------------------------------------------
# ETAPA 6: Validação Final e Relatório
# --------------------------------------------------------------------------------------------------
log_title "ETAPA 6: VERIFICAÇÃO FINAL DOS DADOS CARREGADOS"

sudo -u postgres "$PSQL_BIN" -d "$DB_NAME" << 'EOF' || true
SELECT
    'empresa' AS tabela, COUNT(*) AS total_registros FROM empresa
UNION ALL
SELECT
    'estabelecimento', COUNT(*) FROM estabelecimento
UNION ALL
SELECT
    'socios', COUNT(*) FROM socios
UNION ALL
SELECT
    'simples', COUNT(*) FROM simples
ORDER BY tabela;
EOF

log_title "PROCESSO FINALIZADO COM SUCESSO!"
echo -e "Tempo total decorrido: ${BOLD}${HOURS}h ${MINUTES}m ${SECONDS}s${NC}"
echo -e "Arquivo de log: ${BOLD}${LOG_FILE}${NC}"
echo -e "\nPara consultar via terminal:"
echo -e "  ${CYAN}sudo -u postgres psql -d ${DB_NAME}${NC}\n"
