# -*- coding: utf-8 -*-
"""
Processo de ETL para Coleta e Carga dos Dados Públicos do CNPJ da Receita Federal do Brasil no PostgreSQL.

Atualizado para o novo servidor de Dados Abertos (WebDAV Nextcloud da RFB),
ingestão de alta performance com COPY no PostgreSQL, detecção e atualização automática
de novas versões mensais (sem duplicação de registros) e suporte ao CNPJ Alfanumérico (IN RFB nº 2.229/2024).

Autor original: Aphonso Henrique do Amaral Rafael
Atualizado: 2026
"""

#%%
import argparse
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
# Parâmetros de Linha de Comando
parser = argparse.ArgumentParser(description="ETL Dados Públicos CNPJ - Receita Federal do Brasil")
parser.add_argument("--check-update", "-c", action="store_true", help="Verifica se há nova base na RFB sem baixar arquivos")
parser.add_argument("--force", "-f", action="store_true", help="Força a recarga dos dados mesmo se a versão já constar como concluída no banco")
parser.add_argument("--ano-mes", "-m", type=str, default="", help="Especifica o mês/ano de referência fixo (ex: 2026-08)")
parser.add_argument("--reset", action="store_true", help="Limpa histórico de controle do ETL e recarrega do zero")
cli_args, _ = parser.parse_known_args()

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
    load_dotenv(dotenv_path=dotenv_path)
else:
    load_dotenv()

def getEnv(env_var, default=""):
    val = os.getenv(env_var)
    return val if val is not None else default

output_base_dir = getEnv("OUTPUT_FILES_PATH", "./dados/output_files")
extracted_base_dir = getEnv("EXTRACTED_FILES_PATH", "./dados/extracted_files")
makedirs(output_base_dir)
makedirs(extracted_base_dir)

# Conexão com o Banco de Dados PostgreSQL
db_user = getEnv("DB_USER", "postgres")
db_pass = getEnv("DB_PASSWORD", "postgres")
db_host = getEnv("DB_HOST", "localhost")
db_port = getEnv("DB_PORT", "5432")
db_name = getEnv("DB_NAME", "Dados_RFB")

db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

# Configurações da Receita Federal
share_token = getEnv("RFB_SHARE_TOKEN", "YggdBLfdninEJX9")
webdav_base_url = getEnv("RFB_WEBDAV_URL", "https://arquivos.receitafederal.gov.br/public.php/webdav")
config_ano_mes = cli_args.ano_mes or getEnv("RFB_ANO_MES", "")
pular_download = getEnv("PULAR_DOWNLOAD", "False").lower() in ("true", "1", "sim", "s")
pular_extracao = getEnv("PULAR_EXTRACAO", "False").lower() in ("true", "1", "sim", "s")
force_reload = cli_args.force or getEnv("FORCE_RELOAD", "False").lower() in ("true", "1", "sim", "s")
check_update_mode = cli_args.check_update or getEnv("CHECK_UPDATE", "False").lower() in ("true", "1", "sim", "s")
reset_etl = cli_args.reset or getEnv("RESET_ETL", "False").lower() in ("true", "1", "sim", "s")

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
                sql = f"COPY \"{name}\" ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER \x27;\x27, QUOTE \x27\"\x27, NULL \x27\x27)"
                cur.copy_expert(sql=sql, file=s_buf)
            raw_conn.commit()
        finally:
            raw_conn.close()
    except Exception:
        # Fallback para to_sql padrão do SQLAlchemy
        dataframe.to_sql(name=name, con=engine, if_exists="append", index=False, chunksize=4096)

#%%
# Conexão com o banco de dados
engine = None
conn = None
try:
    engine = create_engine(db_url)
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        host=db_host,
        port=db_port,
        password=db_pass,
    )
except Exception as e:
    print(f"Aviso/Erro na conexão com o banco de dados: {e}")
    print("Verifique se o PostgreSQL está em execução e os dados no arquivo .env.")

#%%
# Inicialização e Migração das Tabelas de Controle e Metadados
def inicializar_tabelas_metadados():
    """Garante que as tabelas _metadados_rfb e _controle_etl existam com versionamento por ano_mes."""
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            # Tabela de metadados da versão RFB
            cur.execute("""
                CREATE TABLE IF NOT EXISTS "_metadados_rfb" (
                    "ano_mes" VARCHAR(7) PRIMARY KEY,
                    "status" VARCHAR(50),
                    "iniciado_em" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    "concluido_em" TIMESTAMP,
                    "total_empresas" BIGINT DEFAULT 0,
                    "total_estabelecimentos" BIGINT DEFAULT 0,
                    "total_socios" BIGINT DEFAULT 0,
                    "total_simples" BIGINT DEFAULT 0
                );
            """)

            # Tabela de controle de arquivos (com chave composta por ano_mes + arquivo)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS "_controle_etl" (
                    "ano_mes" VARCHAR(7) NOT NULL DEFAULT '',
                    "arquivo" VARCHAR(255) NOT NULL,
                    "etapa" VARCHAR(100),
                    "status" VARCHAR(50),
                    "concluido_em" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY ("ano_mes", "arquivo")
                );
            """)

            # Migração suave caso _controle_etl já existisse sem coluna ano_mes
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = '_controle_etl' AND column_name = 'ano_mes'
                    ) THEN
                        ALTER TABLE "_controle_etl" ADD COLUMN "ano_mes" VARCHAR(7) NOT NULL DEFAULT '';
                        ALTER TABLE "_controle_etl" DROP CONSTRAINT IF EXISTS "_controle_etl_pkey";
                        ALTER TABLE "_controle_etl" ADD PRIMARY KEY ("ano_mes", "arquivo");
                    END IF;
                END $$;
            """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Aviso ao inicializar tabelas de controle: {e}")

if conn:
    inicializar_tabelas_metadados()

#%%
# Funções de Consulta de Versão do Banco de Dados
def obter_versao_banco():
    """Retorna o mês/ano (YYYY-MM) atualmente carregado com sucesso no banco, ou None."""
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT "ano_mes", "concluido_em", "total_empresas", "total_estabelecimentos", "total_socios"
                FROM "_metadados_rfb"
                WHERE "status" = 'CONCLUIDO'
                ORDER BY "concluido_em" DESC, "ano_mes" DESC
                LIMIT 1;
            """)
            row = cur.fetchone()
            if row:
                return {
                    "ano_mes": row[0],
                    "concluido_em": row[1],
                    "total_empresas": row[2],
                    "total_estabelecimentos": row[3],
                    "total_socios": row[4],
                }
    except Exception:
        conn.rollback()

    # Fallback: se empresa existe e tem registros mas _metadados_rfb ainda não tinha sido populada
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'empresa');")
            if cur.fetchone()[0]:
                cur.execute("SELECT COUNT(*) FROM empresa;")
                total = cur.fetchone()[0]
                if total > 0:
                    return {
                        "ano_mes": "legado",
                        "concluido_em": None,
                        "total_empresas": total,
                        "total_estabelecimentos": 0,
                        "total_socios": 0,
                    }
    except Exception:
        conn.rollback()

    return None

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
        raise RuntimeError("Nenhuma pasta mensal (YYYY-MM) encontrada no servidor da Receita Federal.")

    mes_selecionado = ano_mes_desejado if ano_mes_desejado else directories[-1]

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
# Verificação de Atualização (Check Update)
def verificar_status_atualizacao():
    """Compara a versão mais recente na RFB com os dados do banco e retorna diagnóstico completo."""
    mes_remoto, download_url, arquivos = listar_arquivos_rfb()
    info_banco = obter_versao_banco()
    mes_banco = info_banco["ano_mes"] if info_banco else None

    tem_atualizacao = False
    if not mes_banco:
        tem_atualizacao = True
        status_desc = "Banco de dados vazio ou sem metadados de versão."
    elif mes_banco == "legado":
        tem_atualizacao = True
        status_desc = f"Banco possui dados legados sem registro de mês. Versão atual na RFB: {mes_remoto}."
    elif mes_banco != mes_remoto:
        tem_atualizacao = True
        status_desc = f"NOVA BASE DISPONÍVEL NA RFB: {mes_remoto} (Banco atual: {mes_banco})."
    else:
        status_desc = f"O banco de dados já está na versão mais recente da Receita Federal ({mes_banco})."

    return {
        "mes_remoto": mes_remoto,
        "mes_banco": mes_banco,
        "tem_atualizacao": tem_atualizacao,
        "status_desc": status_desc,
        "arquivos": arquivos,
        "download_url": download_url,
        "info_banco": info_banco,
    }

# Se for apenas verificação de atualização (--check-update), exibe relatório e finaliza
if check_update_mode:
    print("\n" + "=" * 70)
    print("  VERIFICAÇÃO DE ATUALIZAÇÃO - RECEITA FEDERAL DO BRASIL (CNPJ)")
    print("=" * 70)
    try:
        diag = verificar_status_atualizacao()
        banco_str = diag["mes_banco"] or "Nenhum (Banco Vazio)"
        if diag["info_banco"] and diag["info_banco"].get("total_empresas"):
            banco_str += f" ({diag['info_banco']['total_empresas']:,} empresas carregadas)".replace(",", ".")

        print(f"  Versão atual no Banco:      {banco_str}")
        print(f"  Versão mais recente na RFB: {diag['mes_remoto']} ({len(diag['arquivos'])} arquivos .zip)")
        print("-" * 70)
        if diag["tem_atualizacao"]:
            print(f"  Status: \033[1;33m[!] {diag['status_desc']}\033[0m")
            print("\n  Para atualizar seu banco com a nova versão sem duplicar dados, execute:")
            print("    sudo ./executar_oracle_linux_9.sh --update")
            print("  ou:")
            print("    python3 code/ETL_coletar_dados_e_gravar_BD.py")
            print("=" * 70 + "\n")
            sys.exit(2)
        else:
            print(f"  Status: \033[0;32m[OK] {diag['status_desc']}\033[0m")
            print("  Nenhuma ação necessária no momento.")
            print("=" * 70 + "\n")
            sys.exit(0)
    except Exception as e:
        print(f"  [ERRO] Falha ao verificar atualizações na RFB: {e}")
        print("=" * 70 + "\n")
        sys.exit(1)

#%%
# Obter lista de arquivos para execução normal
if not pular_download:
    print("\nConsultando lista de arquivos na Receita Federal...")
    mes_referencia, base_download_url, files_to_download = listar_arquivos_rfb()
    print(f"Mês/Ano de referência da base: {mes_referencia}")
    print(f"Total de arquivos encontrados ({len(files_to_download)}):")
    for idx, f in enumerate(files_to_download, 1):
        print(f"  {idx:02d} - {f}")
else:
    print("\nPULAR_DOWNLOAD=True: Etapa de consulta e download ignorada.")
    mes_referencia = config_ano_mes or "local"
    files_to_download = []
    base_download_url = ""

# Verifica se o banco já se encontra 100% atualizado com esta versão
info_banco = obter_versao_banco()
mes_banco = info_banco["ano_mes"] if info_banco else None

if mes_banco == mes_referencia and not force_reload and not reset_etl:
    print("\n" + "=" * 70)
    print(f"  [INFO] O banco de dados já se encontra 100% atualizado com a")
    print(f"         versão mais recente da Receita Federal ({mes_referencia}).")
    if info_banco.get("total_empresas"):
        print(f"         Total de empresas cadastradas: {info_banco['total_empresas']:,}".replace(",", "."))
    print(f"  [INFO] Nenhuma nova carga necessária!")
    print(f"  [DICA] Para forçar a recarga desta mesma versão, execute com --force")
    print("=" * 70 + "\n")
    sys.exit(0)

if mes_banco and mes_banco != mes_referencia:
    print("\n" + "=" * 70)
    print(f"  [ATUALIZAÇÃO DE BASE MENSAL DETECTADA]")
    print(f"  Versão atual no Banco:  {mes_banco}")
    print(f"  Nova Versão da RFB:     {mes_referencia}")
    print(f"  O pipeline atualizará os dados com substituição limpa para")
    print(f"  garantir que não ocorra nenhuma duplicação de registros!")
    print("=" * 70 + "\n")

# Diretórios específicos do lote mensal (previne colisão entre meses)
output_files = os.path.join(output_base_dir, mes_referencia)
extracted_files = os.path.join(extracted_base_dir, mes_referencia)
makedirs(output_files)
makedirs(extracted_files)

# Compatibilidade: se arquivos já estiverem na pasta raiz legada de output, aproveita-os
if os.path.isdir(output_base_dir):
    for arq_antigo in os.listdir(output_base_dir):
        origem = os.path.join(output_base_dir, arq_antigo)
        destino = os.path.join(output_files, arq_antigo)
        if os.path.isfile(origem) and arq_antigo.lower().endswith(".zip") and not os.path.exists(destino):
            try:
                os.rename(origem, destino)
            except Exception:
                pass

#%%
# Registra o início do processo na tabela de metadados
if conn:
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO "_metadados_rfb" ("ano_mes", "status", "iniciado_em")
                VALUES (%s, 'EM_ANDAMENTO', NOW())
                ON CONFLICT ("ano_mes") DO UPDATE SET "status" = 'EM_ANDAMENTO';
            """)
        conn.commit()
    except Exception:
        conn.rollback()

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
    print(f"\nIniciando download dos arquivos da Receita Federal ({mes_referencia})...")
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
    print(f"\nIniciando descompactação dos arquivos ({mes_referencia})...")
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

print(f"\nArquivos identificados para carga no banco de dados ({mes_referencia}):")
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

cur = conn.cursor()

#%%
########################################################################################################################
## CONTROLE DE ESTADO E RETOMADA (CHECKPOINTING POR ANO_MES) ###########################################################
########################################################################################################################
if reset_etl:
    print(f"\n[AVISO] RESET_ETL=True detectado: limpando histórico de controle do lote {mes_referencia}.")
    cur.execute('DELETE FROM "_controle_etl" WHERE "ano_mes" = %s;', (mes_referencia,))
    conn.commit()

def arquivo_ja_processado(nome_arquivo, ano_mes=mes_referencia):
    """Verifica se determinado arquivo daquele mês/ano já foi 100% carregado no banco."""
    try:
        cur.execute(
            'SELECT 1 FROM "_controle_etl" WHERE "ano_mes" = %s AND "arquivo" = %s AND "status" = %s;',
            (ano_mes, nome_arquivo, "CONCLUIDO")
        )
        return cur.fetchone() is not None
    except Exception:
        conn.rollback()
        return False

def registrar_arquivo_processado(etapa, nome_arquivo, ano_mes=mes_referencia):
    """Registra que o arquivo daquele mês/ano foi totalmente carregado no banco."""
    try:
        cur.execute(
            'INSERT INTO "_controle_etl" ("ano_mes", "arquivo", "etapa", "status", "concluido_em") '
            'VALUES (%s, %s, %s, %s, NOW()) '
            'ON CONFLICT ("ano_mes", "arquivo") DO UPDATE SET "status" = %s, "concluido_em" = NOW();',
            (ano_mes, nome_arquivo, etapa, "CONCLUIDO", "CONCLUIDO")
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
    print(f"CARGA: EMPRESA ({mes_referencia})")
    print("=" * 60)

    arquivos_pendentes = [f for f in arquivos_empresa if not arquivo_ja_processado(f, mes_referencia)]
    if not arquivos_pendentes:
        print(f"Todos os {len(arquivos_empresa)} arquivos de EMPRESA já foram carregados previamente para a versão {mes_referencia}. Pulando etapa!")
    else:
        arquivos_concluidos = [f for f in arquivos_empresa if arquivo_ja_processado(f, mes_referencia)]
        if not arquivos_concluidos:
            print(f"Substituição limpa da tabela 'empresa' para a versão {mes_referencia} (evita duplicação)...")
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
            registrar_arquivo_processado("empresa", arq, mes_referencia)
            print(f"Arquivo {arq} inserido e registrado com sucesso!")

        print(f"Tempo de carga de Empresas: {round(time.time() - t0)} segundos.")
        gc.collect()

#%%
########################################################################################################################
## CARGA: ESTABELECIMENTOS ############################################################################################
########################################################################################################################
if arquivos_estabelecimento:
    print("\n" + "=" * 60)
    print(f"CARGA: ESTABELECIMENTO ({mes_referencia})")
    print("=" * 60)

    arquivos_pendentes = [f for f in arquivos_estabelecimento if not arquivo_ja_processado(f, mes_referencia)]
    if not arquivos_pendentes:
        print(f"Todos os {len(arquivos_estabelecimento)} arquivos de ESTABELECIMENTO já foram carregados previamente para a versão {mes_referencia}. Pulando etapa!")
    else:
        arquivos_concluidos = [f for f in arquivos_estabelecimento if arquivo_ja_processado(f, mes_referencia)]
        if not arquivos_concluidos:
            print(f"Substituição limpa da tabela 'estabelecimento' para a versão {mes_referencia} (evita duplicação)...")
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
            registrar_arquivo_processado("estabelecimento", arq, mes_referencia)
            print(f"Arquivo {arq} inserido e registrado com sucesso!")

        print(f"Tempo de carga de Estabelecimentos: {round(time.time() - t0)} segundos.")
        gc.collect()

#%%
########################################################################################################################
## CARGA: SÓCIOS #######################################################################################################
########################################################################################################################
if arquivos_socios:
    print("\n" + "=" * 60)
    print(f"CARGA: SÓCIOS ({mes_referencia})")
    print("=" * 60)

    arquivos_pendentes = [f for f in arquivos_socios if not arquivo_ja_processado(f, mes_referencia)]
    if not arquivos_pendentes:
        print(f"Todos os {len(arquivos_socios)} arquivos de SÓCIOS já foram carregados previamente para a versão {mes_referencia}. Pulando etapa!")
    else:
        arquivos_concluidos = [f for f in arquivos_socios if arquivo_ja_processado(f, mes_referencia)]
        if not arquivos_concluidos:
            print(f"Substituição limpa da tabela 'socios' para a versão {mes_referencia} (evita duplicação)...")
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
            registrar_arquivo_processado("socios", arq, mes_referencia)
            print(f"Arquivo {arq} inserido e registrado com sucesso!")

        print(f"Tempo de carga de Sócios: {round(time.time() - t0)} segundos.")
        gc.collect()

#%%
########################################################################################################################
## CARGA: SIMPLES NACIONAL #############################################################################################
########################################################################################################################
if arquivos_simples:
    print("\n" + "=" * 60)
    print(f"CARGA: SIMPLES NACIONAL / MEI ({mes_referencia})")
    print("=" * 60)

    arquivos_pendentes = [f for f in arquivos_simples if not arquivo_ja_processado(f, mes_referencia)]
    if not arquivos_pendentes:
        print(f"Todos os {len(arquivos_simples)} arquivos de SIMPLES NACIONAL já foram carregados previamente para a versão {mes_referencia}. Pulando etapa!")
    else:
        arquivos_concluidos = [f for f in arquivos_simples if arquivo_ja_processado(f, mes_referencia)]
        if not arquivos_concluidos:
            print(f"Substituição limpa da tabela 'simples' para a versão {mes_referencia} (evita duplicação)...")
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
            registrar_arquivo_processado("simples", arq, mes_referencia)
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

    arquivos_pendentes = [f for f in arquivos if not arquivo_ja_processado(f, mes_referencia)]
    if not arquivos_pendentes:
        print(f"Domínio {nome_tabela.upper()} ({mes_referencia}) já carregado previamente no banco. Pulando!")
        return

    print(f"\nProcessando domínio: {nome_tabela.upper()} ({mes_referencia}) [...]")
    t0 = time.time()
    cur.execute(f'DROP TABLE IF EXISTS "{nome_tabela}";')
    conn.commit()

    for arq in arquivos_pendentes:
        caminho = os.path.join(extracted_files, arq)
        df = pd.read_csv(caminho, sep=";", header=None, dtype=dtypes, encoding="latin-1")
        df.columns = colunas
        to_sql_fast(df, name=nome_tabela, engine=engine)
        registrar_arquivo_processado(nome_tabela, arq, mes_referencia)
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
if arquivo_ja_processado("INDICES_CRIADOS", mes_referencia):
    print(f"\nÍndices de alta performance já foram criados previamente para {mes_referencia}. Pulando criação de índices.")
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
    registrar_arquivo_processado("indices", "INDICES_CRIADOS", mes_referencia)

    print(f"Índices criados com sucesso em {round(time.time() - t_idx, 2)} segundos!")

#%%
########################################################################################################################
## FINALIZAÇÃO E REGISTRO DE METADADOS DA VERSÃO #######################################################################
########################################################################################################################
print("\nRegistrando versão concluída nos metadados do banco...")
try:
    cur.execute('SELECT COUNT(*) FROM "empresa";')
    total_emp = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM "estabelecimento";')
    total_est = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM "socios";')
    total_soc = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM "simples";')
    total_sim = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO "_metadados_rfb" (
            "ano_mes", "status", "concluido_em",
            "total_empresas", "total_estabelecimentos", "total_socios", "total_simples"
        )
        VALUES (%s, 'CONCLUIDO', NOW(), %s, %s, %s, %s)
        ON CONFLICT ("ano_mes") DO UPDATE SET
            "status" = 'CONCLUIDO',
            "concluido_em" = NOW(),
            "total_empresas" = EXCLUDED."total_empresas",
            "total_estabelecimentos" = EXCLUDED."total_estabelecimentos",
            "total_socios" = EXCLUDED."total_socios",
            "total_simples" = EXCLUDED."total_simples";
    """, (mes_referencia, total_emp, total_est, total_soc, total_sim))
    conn.commit()

    print("\n" + "=" * 70)
    print("  ATUALIZAÇÃO DE BASE CONCLUÍDA COM SUCESSO!")
    print("=" * 70)
    print(f"  Versão da Base RFB:          {mes_referencia}")
    print(f"  Total de Empresas:           {total_emp:,}".replace(",", "."))
    print(f"  Total de Estabelecimentos:   {total_est:,}".replace(",", "."))
    print(f"  Total de Sócios (QSA):       {total_soc:,}".replace(",", "."))
    print(f"  Total de Simples / MEI:      {total_sim:,}".replace(",", "."))
    print("=" * 70)
except Exception as e:
    conn.rollback()
    print(f"Aviso ao registrar metadados finais da versão: {e}")

print("\nProcesso de ETL 100% finalizado! Os dados estão prontos para consulta no PostgreSQL e na Interface Web.")
