# -*- coding: utf-8 -*-
"""
Camada de Acesso a Dados do PostgreSQL para o Painel Web CNPJ.
Inclui pool de conexões, cache de tabelas de domínio em memória,
consultas parametrizadas de alta performance com paginação e exportação CSV / XLS (Excel).
"""

import csv
import io
import os
import pathlib
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

# Localiza e carrega o arquivo .env
def load_environment():
    candidates = [
        pathlib.Path().resolve() / ".env",
        pathlib.Path(__file__).resolve().parent / ".env",
        pathlib.Path(__file__).resolve().parent.parent / ".env",
        pathlib.Path(__file__).resolve().parent.parent / "code" / ".env",
    ]
    for p in candidates:
        if p.is_file():
            load_dotenv(dotenv_path=p)
            return str(p)
    load_dotenv()
    return None

load_environment()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "Dados_RFB")

# Pool de conexões seguro para multi-threading
connection_pool = None

def init_pool():
    global connection_pool
    if connection_pool is None:
        try:
            connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=20,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                connect_timeout=5,
            )
        except Exception as e:
            print(f"[Aviso Banco de Dados] Não foi possível iniciar o pool de conexões: {e}")
            connection_pool = None

def get_connection():
    global connection_pool
    if connection_pool is None:
        init_pool()
    if connection_pool:
        return connection_pool.getconn()
    # Fallback caso o pool falhe
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        connect_timeout=5,
    )

def release_connection(conn):
    global connection_pool
    if connection_pool and conn:
        try:
            connection_pool.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    elif conn:
        try:
            conn.close()
        except Exception:
            pass

# --------------------------------------------------------------------------------------------------
# Cache em Memória das Tabelas de Domínio
# --------------------------------------------------------------------------------------------------
CACHE_DOMINIOS = {
    "cnae": {},
    "munic": {},
    "natju": {},
    "moti": {},
    "pais": {},
    "quals": {},
}

MAPA_SITUACAO = {
    1: "Nula",
    2: "Ativa",
    3: "Suspensa",
    4: "Inapta",
    8: "Baixada",
}

MAPA_PORTE = {
    1: "Não Informado",
    2: "Microempresa (ME)",
    3: "Empresa de Pequeno Porte (EPP)",
    5: "Demais",
}

MAPA_MATRIZ_FILIAL = {
    1: "Matriz",
    2: "Filial",
}

LISTA_UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
    "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
    "RO", "RR", "RS", "SC", "SE", "SP", "TO", "EX",
]

# --------------------------------------------------------------------------------------------------
# Ordenação dos Resultados
# Whitelist rígida (chave da interface -> cláusula SQL) para evitar injeção de SQL via ORDER BY.
# A "data de criação da empresa" é representada por est.data_inicio_atividade (formato YYYYMMDD),
# que ordena corretamente de forma lexicográfica. O cast para texto mantém a ordenação correta
# tanto no schema do ETL (coluna INTEGER) quanto no DDL (VARCHAR), e NULLIF trata campos vazios
# e nulos como "sem data" (sempre ao final da listagem).
# --------------------------------------------------------------------------------------------------
ORDENACAO_PADRAO = "cnpj_asc"

MAPA_ORDENACAO = {
    "cnpj_asc": "est.cnpj_basico ASC, est.cnpj_ordem ASC",
    "data_criacao_desc": "NULLIF(est.data_inicio_atividade::text, '') DESC NULLS LAST, est.cnpj_basico ASC, est.cnpj_ordem ASC",
    "data_criacao_asc": "NULLIF(est.data_inicio_atividade::text, '') ASC NULLS LAST, est.cnpj_basico ASC, est.cnpj_ordem ASC",
    "razao_social_asc": "emp.razao_social ASC NULLS LAST, est.cnpj_basico ASC, est.cnpj_ordem ASC",
    "razao_social_desc": "emp.razao_social DESC NULLS LAST, est.cnpj_basico ASC, est.cnpj_ordem ASC",
    "capital_social_desc": "emp.capital_social DESC NULLS LAST, est.cnpj_basico ASC, est.cnpj_ordem ASC",
    "capital_social_asc": "emp.capital_social ASC NULLS LAST, est.cnpj_basico ASC, est.cnpj_ordem ASC",
}

def resolve_ordenacao(ordenar_por):
    """Traduz a opção de ordenação informada em uma cláusula ORDER BY segura (whitelist)."""
    chave = str(ordenar_por or "").strip().lower()
    return MAPA_ORDENACAO.get(chave, MAPA_ORDENACAO[ORDENACAO_PADRAO])

# --------------------------------------------------------------------------------------------------
# Limite de Resultados
# Permite trazer apenas os N primeiros registros que atendem aos filtros (ex: 100), evitando
# varrer e transferir milhões de linhas em consultas amplas. Vale também para as exportações.
# --------------------------------------------------------------------------------------------------
LIMITE_MAXIMO = 10000

def resolve_limite(valor, maximo=LIMITE_MAXIMO):
    """Converte o limite informado em inteiro positivo; devolve None se vazio ou inválido."""
    texto = str(valor or "").strip()
    if not texto:
        return None
    try:
        numero = int(float(texto))
    except (TypeError, ValueError):
        return None
    if numero <= 0:
        return None
    return min(numero, maximo)

def load_domain_caches():
    """Carrega pequenas tabelas de domínio na memória para aceleração extrema de consultas."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            for tabela in ["cnae", "munic", "natju", "moti", "pais", "quals"]:
                try:
                    cur.execute(f'SELECT "codigo", "descricao" FROM "{tabela}" LIMIT 10000;')
                    rows = cur.fetchall()
                    CACHE_DOMINIOS[tabela] = {str(r[0]): r[1] for r in rows if r[0] is not None}
                except Exception:
                    conn.rollback()
    except Exception as e:
        print(f"[Aviso] Não foi possível carregar domínios em memória: {e}")
    finally:
        if conn:
            release_connection(conn)

# --------------------------------------------------------------------------------------------------
# Formatações Utilitárias
# --------------------------------------------------------------------------------------------------
def format_cnpj(basico, ordem, dv):
    if not basico:
        return ""
    b = str(basico).zfill(8)
    o = str(ordem or "0001").zfill(4)
    d = str(dv or "00").zfill(2)
    return f"{b[:2]}.{b[2:5]}.{b[5:8]}/{o}-{d}"

def format_date(dt_str):
    if not dt_str or len(str(dt_str)) != 8:
        return dt_str or ""
    s = str(dt_str)
    return f"{s[6:8]}/{s[4:6]}/{s[:4]}"

def format_codigo(valor, tamanho):
    """Normaliza códigos numéricos (ex: CNAE) preservando zeros à esquerda.

    No schema gerado pelo to_sql do ETL essas colunas são INTEGER, o que descarta o zero
    inicial (0111301 -> 111301) e impede a busca na tabela de domínio. O zfill restaura o
    código original; valores não numéricos ou já preenchidos são devolvidos como estão.
    """
    if valor is None:
        return ""
    codigo = str(valor).strip()
    return codigo.zfill(tamanho) if codigo.isdigit() else codigo

def format_currency(val):
    if val is None:
        return "R$ 0,00"
    try:
        return f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(val)

# --------------------------------------------------------------------------------------------------
# Consulta Parametrizada de Empresas com Paginação
# --------------------------------------------------------------------------------------------------
def search_empresas(filters, page=1, page_size=25):
    """
    Busca empresas com suporte a múltiplos filtros e paginação no PostgreSQL.
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            where_clauses = []
            params = []

            # 1. Filtro de CNPJ
            cnpj_raw = filters.get("cnpj", "").strip()
            if cnpj_raw:
                cnpj_clean = cnpj_raw.replace(".", "").replace("/", "").replace("-", "")
                if len(cnpj_clean) == 14:
                    where_clauses.append("(est.cnpj_basico = %s AND est.cnpj_ordem = %s AND est.cnpj_dv = %s)")
                    params.extend([cnpj_clean[:8], cnpj_clean[8:12], cnpj_clean[12:14]])
                elif len(cnpj_clean) == 8:
                    where_clauses.append("est.cnpj_basico = %s")
                    params.append(cnpj_clean)
                else:
                    where_clauses.append("est.cnpj_basico LIKE %s")
                    params.append(f"{cnpj_clean}%")

            # 2. Razão Social
            razao_social = filters.get("razao_social", "").strip()
            if razao_social:
                where_clauses.append("emp.razao_social ILIKE %s")
                params.append(f"%{razao_social}%")

            # 3. Nome Fantasia
            nome_fantasia = filters.get("nome_fantasia", "").strip()
            if nome_fantasia:
                where_clauses.append("est.nome_fantasia ILIKE %s")
                params.append(f"%{nome_fantasia}%")

            # 4. UF
            uf = filters.get("uf", "").strip().upper()
            if uf and uf != "TODOS":
                where_clauses.append("est.uf = %s")
                params.append(uf)

            # 5. Município
            municipio = filters.get("municipio", "").strip()
            if municipio:
                if municipio.isdigit():
                    where_clauses.append("est.municipio = %s")
                    params.append(int(municipio))
                else:
                    # Tenta localizar código no cache de municípios
                    codigos = [k for k, v in CACHE_DOMINIOS["munic"].items() if municipio.lower() in v.lower()]
                    if codigos:
                        where_clauses.append("est.municipio = ANY(%s)")
                        params.append([int(c) for c in codigos[:50] if c.isdigit()])

            # 6. Situação Cadastral
            situacao = filters.get("situacao_cadastral", "").strip()
            if situacao and situacao != "TODOS":
                where_clauses.append("est.situacao_cadastral = %s")
                params.append(int(situacao))

            # 7. Tipo Matriz / Filial
            tipo_matriz = filters.get("matriz_filial", "").strip()
            if tipo_matriz and tipo_matriz != "TODOS":
                where_clauses.append("est.identificador_matriz_filial = %s")
                params.append(int(tipo_matriz))

            # 8. Porte da Empresa
            porte = filters.get("porte_empresa", "").strip()
            if porte and porte != "TODOS":
                where_clauses.append("emp.porte_empresa = %s")
                params.append(int(porte))

            # 9. CNAE Principal
            # A coluna pode ser INTEGER (schema gerado pelo to_sql do ETL) ou VARCHAR (DDL).
            # A conversão para texto com lpad mantém o filtro funcionando nos dois formatos e
            # preserva os zeros à esquerda dos códigos CNAE (ex: 0111301).
            cnae = filters.get("cnae", "").strip().replace("-", "").replace("/", "")
            if cnae:
                where_clauses.append("lpad(est.cnae_fiscal_principal::text, 7, '0') LIKE %s")
                params.append(f"{cnae}%")

            # 10. Natureza Jurídica
            natju = filters.get("natureza_juridica", "").strip()
            if natju and natju != "TODOS":
                where_clauses.append("emp.natureza_juridica = %s")
                params.append(int(natju))

            # 11. Opção Simples Nacional
            simples = filters.get("opcao_simples", "").strip().upper()
            if simples in ("S", "SIM"):
                where_clauses.append("sim.opcao_pelo_simples IN ('S', 'SIM')")
            elif simples in ("N", "NAO", "NÃO"):
                where_clauses.append("(sim.opcao_pelo_simples IS NULL OR sim.opcao_pelo_simples IN ('N', 'NAO'))")

            # 12. Opção MEI
            mei = filters.get("opcao_mei", "").strip().upper()
            if mei in ("S", "SIM"):
                where_clauses.append("sim.opcao_mei IN ('S', 'SIM')")
            elif mei in ("N", "NAO", "NÃO"):
                where_clauses.append("(sim.opcao_mei IS NULL OR sim.opcao_mei IN ('N', 'NAO'))")

            # 13. Faixa de Capital Social
            cap_min = filters.get("capital_min", "").strip()
            if cap_min:
                try:
                    where_clauses.append("emp.capital_social >= %s")
                    params.append(float(cap_min))
                except ValueError:
                    pass

            cap_max = filters.get("capital_max", "").strip()
            if cap_max:
                try:
                    where_clauses.append("emp.capital_social <= %s")
                    params.append(float(cap_max))
                except ValueError:
                    pass

            # 14. Data de Início de Atividade
            # A comparação com o literal YYYYMMDD funciona nos dois schemas (coluna INTEGER do
            # ETL ou VARCHAR do DDL), pois o PostgreSQL converte o parâmetro para o tipo da coluna.
            dt_ini = filters.get("data_inicio_de", "").strip().replace("-", "").replace("/", "")
            if dt_ini:
                where_clauses.append("est.data_inicio_atividade >= %s")
                params.append(dt_ini)

            dt_fim = filters.get("data_inicio_ate", "").strip().replace("-", "").replace("/", "")
            if dt_fim:
                where_clauses.append("est.data_inicio_atividade <= %s")
                # Em colunas VARCHAR um registro sem data ("") casaria com "" <= "20211231";
                # o predicado abaixo garante que apenas empresas com data preenchida retornem.
                where_clauses.append("(est.data_inicio_atividade::text <> '')")
                params.append(dt_fim)

            where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

            # 15. Ordenação dos resultados (ex: por data de criação da empresa)
            order_sql = resolve_ordenacao(filters.get("ordenar_por", ""))

            # 16. Limite de resultados (ex: trazer apenas 100 registros)
            limite = resolve_limite(filters.get("limite", ""))

            # Contagem total de registros (com teto para performance se for consulta muito ampla).
            # Quando há limite, a contagem para no próprio limite: além de mais rápido, o total
            # exibido passa a refletir exatamente quantos registros serão apresentados.
            count_cap = min(limite, 10001) if limite else 10001
            count_query = f"""
                SELECT COUNT(*) FROM (
                    SELECT 1
                    FROM "estabelecimento" est
                    INNER JOIN "empresa" emp ON emp.cnpj_basico = est.cnpj_basico
                    LEFT JOIN "simples" sim ON sim.cnpj_basico = est.cnpj_basico
                    {where_sql}
                    LIMIT {int(count_cap)}
                ) AS sub;
            """
            cur.execute(count_query, params)
            total_records = cur.fetchone()[0]

            # Paginação
            page = max(1, int(page))
            page_size = max(10, min(100, int(page_size)))
            page_size_pagina = page_size
            offset = (page - 1) * page_size

            # O limite incide sobre o total de registros, não sobre a página: a última página
            # devolve apenas o que falta para completar o limite (e nada além dele).
            if limite:
                page_size_pagina = max(0, min(page_size, limite - offset))

            # Consulta paginada dos dados
            data_query = f"""
                SELECT
                    est.cnpj_basico,
                    est.cnpj_ordem,
                    est.cnpj_dv,
                    emp.razao_social,
                    est.nome_fantasia,
                    est.situacao_cadastral,
                    est.data_situacao_cadastral,
                    est.data_inicio_atividade,
                    est.cnae_fiscal_principal,
                    est.uf,
                    est.municipio,
                    est.identificador_matriz_filial,
                    emp.porte_empresa,
                    emp.capital_social,
                    emp.natureza_juridica,
                    sim.opcao_pelo_simples,
                    sim.opcao_mei,
                    est.logradouro,
                    est.numero,
                    est.bairro,
                    est.cep,
                    est.ddd_1,
                    est.telefone_1,
                    est.correio_eletronico
                FROM "estabelecimento" est
                INNER JOIN "empresa" emp ON emp.cnpj_basico = est.cnpj_basico
                LEFT JOIN "simples" sim ON sim.cnpj_basico = est.cnpj_basico
                {where_sql}
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s;
            """
            cur.execute(data_query, params + [page_size_pagina, offset])
            rows = cur.fetchall()

            results = []
            for r in rows:
                cnae_cod = format_codigo(r[8], 7)
                cnae_desc = CACHE_DOMINIOS["cnae"].get(cnae_cod, "")
                munic_cod = str(r[10] or "")
                munic_nome = CACHE_DOMINIOS["munic"].get(munic_cod, "")
                natju_cod = str(r[14] or "")
                natju_desc = CACHE_DOMINIOS["natju"].get(natju_cod, "")

                results.append({
                    "cnpj": format_cnpj(r[0], r[1], r[2]),
                    "cnpj_basico": r[0],
                    "cnpj_ordem": r[1],
                    "cnpj_dv": r[2],
                    "razao_social": r[3] or "",
                    "nome_fantasia": r[4] or "",
                    "situacao_cadastral": MAPA_SITUACAO.get(r[5], f"Código {r[5]}"),
                    "situacao_cod": r[5],
                    "data_situacao": format_date(r[6]),
                    "data_inicio": format_date(r[7]),
                    "cnae_codigo": cnae_cod,
                    "cnae_descricao": cnae_desc,
                    "uf": r[9] or "",
                    "municipio": munic_nome or munic_cod,
                    "tipo": MAPA_MATRIZ_FILIAL.get(r[11], "Desconhecido"),
                    "porte": MAPA_PORTE.get(r[12], "Demais"),
                    "capital_social": format_currency(r[13]),
                    "natureza_juridica": natju_desc or natju_cod,
                    "simples": "Sim" if r[15] in ("S", "SIM") else "Não",
                    "mei": "Sim" if r[16] in ("S", "SIM") else "Não",
                    "endereco_resumo": f"{r[17] or ''}, {r[18] or ''} - {r[19] or ''}, {r[9] or ''}",
                    "telefone": f"({r[21]}) {r[22]}" if r[21] and r[22] else "",
                    "email": r[23] or "",
                })

            total_pages = max(1, (total_records + page_size - 1) // page_size)
            is_capped = total_records > 10000

            return {
                "results": results,
                "total_records": total_records,
                "is_capped": is_capped,
                "limite": limite,
                "limite_aplicado": bool(limite),
                "limite_atingido": bool(limite and total_records >= limite),
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
            }
    except Exception as e:
        print(f"[Erro de Busca] {e}")
        return {
            "results": [],
            "total_records": 0,
            "page": page,
            "page_size": page_size,
            "total_pages": 1,
            "error": str(e),
        }
    finally:
        if conn:
            release_connection(conn)

# --------------------------------------------------------------------------------------------------
# Detalhes Completos da Empresa (Modal / Ficha Cadastral)
# --------------------------------------------------------------------------------------------------
def get_empresa_details(cnpj_basico):
    """Retorna a ficha cadastral completa da empresa, incluindo matriz, filiais e sócios (QSA)."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # 1. Dados da Empresa
            cur.execute("""
                SELECT cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
                       capital_social, porte_empresa, ente_federativo_responsavel
                FROM "empresa" WHERE cnpj_basico = %s;
            """, (cnpj_basico,))
            emp = cur.fetchone()
            if not emp:
                return None

            # 2. Dados de Estabelecimentos (Matriz e Filiais)
            cur.execute("""
                SELECT cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
                       situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral,
                       data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
                       tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio,
                       ddd_1, telefone_1, ddd_2, telefone_2, correio_eletronico, situacao_especial, data_situacao_especial
                FROM "estabelecimento" WHERE cnpj_basico = %s
                ORDER BY identificador_matriz_filial ASC, cnpj_ordem ASC LIMIT 50;
            """, (cnpj_basico,))
            estabelecimentos = cur.fetchall()

            # 3. Dados de Sócios (QSA)
            cur.execute("""
                SELECT identificador_socio, nome_socio_razao_social, cpf_cnpj_socio,
                       qualificacao_socio, data_entrada_sociedade, pais, representante_legal,
                       nome_do_representante, qualificacao_representante_legal, faixa_etaria
                FROM "socios" WHERE cnpj_basico = %s
                ORDER BY nome_socio_razao_social ASC LIMIT 100;
            """, (cnpj_basico,))
            socios = cur.fetchall()

            # 4. Dados do Simples / MEI
            cur.execute("""
                SELECT opcao_pelo_simples, data_opcao_simples, data_exclusao_simples,
                       opcao_mei, data_opcao_mei, data_exclusao_mei
                FROM "simples" WHERE cnpj_basico = %s;
            """, (cnpj_basico,))
            sim = cur.fetchone()

            # Processa dados estruturados
            matriz = estabelecimentos[0] if estabelecimentos else None
            cnae_principal = format_codigo(matriz[9], 7) if matriz else ""
            cnae_desc = CACHE_DOMINIOS["cnae"].get(cnae_principal, "")
            munic_nome = CACHE_DOMINIOS["munic"].get(str(matriz[18]), "") if matriz else ""
            natju_desc = CACHE_DOMINIOS["natju"].get(str(emp[2]), "")

            # Formata lista de estabelecimentos
            est_list = []
            for e in estabelecimentos:
                est_list.append({
                    "cnpj": format_cnpj(e[0], e[1], e[2]),
                    "ordem": e[1],
                    "tipo": MAPA_MATRIZ_FILIAL.get(e[3], "Filial"),
                    "nome_fantasia": e[4] or "",
                    "situacao": MAPA_SITUACAO.get(e[5], f"Cód {e[5]}"),
                    "data_situacao": format_date(e[6]),
                    "cnae": format_codigo(e[9], 7),
                    "endereco": f"{e[11] or ''} {e[12] or ''}, {e[13] or ''} - {e[15] or ''}, {CACHE_DOMINIOS['munic'].get(str(e[18]), '')} - {e[17] or ''} (CEP: {e[16] or ''})",
                    "contato": f"({e[19]}) {e[20]}" if e[19] and e[20] else e[23] or "",
                })

            # Formata lista de sócios
            soc_list = []
            for s in socios:
                soc_list.append({
                    "nome": s[1] or "",
                    "tipo_pessoa": "Pessoa Física" if s[0] == 2 else ("Pessoa Jurídica" if s[0] == 1 else "Estrangeiro"),
                    "documento": s[2] or "",
                    "qualificacao": CACHE_DOMINIOS["quals"].get(str(s[3]), f"Cód {s[3]}"),
                    "data_entrada": format_date(s[4]),
                    "faixa_etaria": f"Faixa {s[9]}" if s[9] else "Não informada",
                    "representante": s[7] or "",
                })

            return {
                "cnpj_basico": emp[0],
                "razao_social": emp[1] or "",
                "cnpj_completo": format_cnpj(matriz[0], matriz[1], matriz[2]) if matriz else emp[0],
                "natureza_juridica": natju_desc or str(emp[2]),
                "porte": MAPA_PORTE.get(emp[5], "Demais"),
                "capital_social": format_currency(emp[4]),
                "ente_federativo": emp[6] or "Não se aplica",
                "cnae_principal": f"{cnae_principal} - {cnae_desc}" if cnae_desc else cnae_principal,
                "cnae_secundario": matriz[10] if matriz else "",
                "situacao_cadastral": MAPA_SITUACAO.get(matriz[5], "") if matriz else "",
                "data_situacao": format_date(matriz[6]) if matriz else "",
                "data_inicio": format_date(matriz[8]) if matriz else "",
                "endereco": f"{matriz[11] or ''} {matriz[12] or ''}, {matriz[13] or ''} {matriz[14] or ''}".strip() if matriz else "",
                "bairro": matriz[15] if matriz else "",
                "cep": matriz[16] if matriz else "",
                "uf": matriz[17] if matriz else "",
                "municipio": munic_nome,
                "telefone": f"({matriz[19]}) {matriz[20]}" if matriz and matriz[19] and matriz[20] else "",
                "email": matriz[23] if matriz else "",
                "simples_optante": "Sim" if sim and sim[0] in ("S", "SIM") else "Não",
                "simples_data_opcao": format_date(sim[1]) if sim and sim[1] else "-",
                "simples_data_exclusao": format_date(sim[2]) if sim and sim[2] else "-",
                "mei_optante": "Sim" if sim and sim[3] in ("S", "SIM") else "Não",
                "mei_data_opcao": format_date(sim[4]) if sim and sim[4] else "-",
                "estabelecimentos": est_list,
                "socios": soc_list,
            }
    except Exception as e:
        print(f"[Erro Detalhes Empresa] {e}")
        return None
    finally:
        if conn:
            release_connection(conn)

# --------------------------------------------------------------------------------------------------
# Exportação Dinâmica para CSV
# --------------------------------------------------------------------------------------------------
def generate_csv_stream(filters, max_rows=10000):
    """Gera um fluxo de linhas CSV com UTF-8-BOM e delimitador ';' compatível com Excel."""
    res = search_empresas(filters, page=1, page_size=max_rows)
    items = res.get("results", [])

    output = io.StringIO()
    # Adiciona UTF-8 BOM
    output.write("\ufeff")
    writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_MINIMAL)

    headers = [
        "CNPJ", "Razão Social", "Nome Fantasia", "Tipo", "Situação Cadastral",
        "Data Situação", "Data Início", "CNAE Código", "CNAE Descrição",
        "UF", "Município", "Porte", "Capital Social", "Natureza Jurídica",
        "Simples Nacional", "MEI", "Endereço", "Telefone", "E-mail"
    ]
    writer.writerow(headers)
    yield output.getvalue()
    output.seek(0)
    output.truncate(0)

    for item in items:
        writer.writerow([
            item["cnpj"],
            item["razao_social"],
            item["nome_fantasia"],
            item["tipo"],
            item["situacao_cadastral"],
            item["data_situacao"],
            item["data_inicio"],
            item["cnae_codigo"],
            item["cnae_descricao"],
            item["uf"],
            item["municipio"],
            item["porte"],
            item["capital_social"],
            item["natureza_juridica"],
            item["simples"],
            item["mei"],
            item["endereco_resumo"],
            item["telefone"],
            item["email"]
        ])
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

# --------------------------------------------------------------------------------------------------
# Exportação Direta para Planilha Excel (XLS / XLSX)
# --------------------------------------------------------------------------------------------------
def generate_excel_file(filters, max_rows=10000):
    """Gera uma planilha Excel (.xlsx / .xls) em memória com formatação e estilização profissional."""
    import openpyxl
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Font, PatternFill, Alignment

    res = search_empresas(filters, page=1, page_size=max_rows)
    items = res.get("results", [])

    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet(title="Empresas RFB")

    # Define larguras ajustadas para cada coluna
    col_widths = {
        "A": 22,  # CNPJ
        "B": 38,  # Razão Social
        "C": 32,  # Nome Fantasia
        "D": 14,  # Tipo
        "E": 18,  # Situação Cadastral
        "F": 15,  # Data Situação
        "G": 15,  # Data Início
        "H": 15,  # CNAE Código
        "I": 40,  # CNAE Descrição
        "J": 8,   # UF
        "K": 26,  # Município
        "L": 14,  # Porte
        "M": 22,  # Capital Social
        "N": 38,  # Natureza Jurídica
        "O": 18,  # Simples Nacional
        "P": 12,  # MEI
        "Q": 44,  # Endereço
        "R": 18,  # Telefone
        "S": 32,  # E-mail
    }
    for col, width in col_widths.items():
        ws.column_dimensions[col].width = width

    headers = [
        "CNPJ", "Razão Social", "Nome Fantasia", "Tipo", "Situação Cadastral",
        "Data Situação", "Data Início", "CNAE Código", "CNAE Descrição",
        "UF", "Município", "Porte", "Capital Social", "Natureza Jurídica",
        "Simples Nacional", "MEI", "Endereço", "Telefone", "E-mail"
    ]

    header_font = Font(name="Segoe UI", size=11, bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="18181B", end_color="18181B", fill_type="solid")
    header_align = Alignment(horizontal="center", vertical="center")

    header_row = []
    for h in headers:
        c = WriteOnlyCell(ws, value=h)
        c.font = header_font
        c.fill = header_fill
        c.alignment = header_align
        header_row.append(c)
    ws.append(header_row)

    for item in items:
        ws.append([
            item.get("cnpj", ""),
            item.get("razao_social", ""),
            item.get("nome_fantasia", ""),
            item.get("tipo", ""),
            item.get("situacao_cadastral", ""),
            item.get("data_situacao", ""),
            item.get("data_inicio", ""),
            item.get("cnae_codigo", ""),
            item.get("cnae_descricao", ""),
            item.get("uf", ""),
            item.get("municipio", ""),
            item.get("porte", ""),
            item.get("capital_social", ""),
            item.get("natureza_juridica", ""),
            item.get("simples", ""),
            item.get("mei", ""),
            item.get("endereco_resumo", ""),
            item.get("telefone", ""),
            item.get("email", ""),
        ])

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output

def get_rfb_metadata():
    """Retorna metadados da base RFB atualmente carregada no banco (_metadados_rfb)."""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT "ano_mes", "status", "concluido_em", "total_empresas", "total_estabelecimentos", "total_socios"
                FROM "_metadados_rfb"
                WHERE "status" = 'CONCLUIDO'
                ORDER BY "concluido_em" DESC, "ano_mes" DESC
                LIMIT 1;
            """)
            row = cur.fetchone()
            if row:
                return {
                    "ano_mes": row[0],
                    "status": row[1],
                    "concluido_em": row[2].strftime("%d/%m/%Y %H:%M") if row[2] else None,
                    "total_empresas": row[3],
                    "total_estabelecimentos": row[4],
                    "total_socios": row[5],
                }
    except Exception:
        pass
    return None

# Inicializa pool e caches na importação
init_pool()
load_domain_caches()
