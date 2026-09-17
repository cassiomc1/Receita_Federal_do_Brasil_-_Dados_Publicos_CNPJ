# -*- coding: utf-8 -*-
"""
Módulo de Inteligência Artificial com TypeSafe AI (System One / Jev).

Fornece serviços de:
1. Interpretação de consultas em linguagem natural e conversão em filtros estruturados.
2. Diagnóstico cadastral de risco e conformidade (Due Diligence / KYC) para empresas.
3. Recomendação e mapeamento semântico de CNAEs com base em descrições de negócios.
"""

import os
import re
import unicodedata
from datetime import datetime
from dotenv import load_dotenv

# Tenta carregar variáveis de ambiente do .env
try:
    from web.database import load_environment, CACHE_DOMINIOS, LISTA_UFS
    load_environment()
except ImportError:
    try:
        from database import load_environment, CACHE_DOMINIOS, LISTA_UFS
        load_environment()
    except Exception:
        load_dotenv()
        CACHE_DOMINIOS = {"cnae": {}, "munic": {}}
        LISTA_UFS = [
            "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
            "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
            "RO", "RR", "RS", "SC", "SE", "SP", "TO", "EX"
        ]

# Tentativa de import do SDK TypeSafe
try:
    from typesafe_sdk import TypeSafeClient, Choice, Noul, Score
    TYPESAFE_SDK_AVAILABLE = True
except ImportError:
    TYPESAFE_SDK_AVAILABLE = False
    TypeSafeClient = None
    Choice = None
    Noul = None
    Score = None


class TypeSafeNotConfiguredError(RuntimeError):
    """Lançada quando a chave TYPESAFE_API_KEY não está configurada no ambiente."""
    pass


def get_typesafe_api_key() -> str:
    """Retorna a chave de API do TypeSafe AI configurada."""
    key = os.getenv("TYPESAFE_API_KEY", "").strip()
    if key in ("sua_chave_typesafe_aqui", "sua_chave_aqui", "seu_token_aqui"):
        return ""
    return key


def is_ai_configured() -> bool:
    """Indica se o TypeSafe SDK está instalado e a chave de API configurada."""
    return bool(TYPESAFE_SDK_AVAILABLE and get_typesafe_api_key())


def _extrair_valor_monetario_texto(texto: str) -> float | None:
    """Extrai limites de valor monetário de texto como 'acima de 500k', 'superior a 50 mil', 'mais de 1 milhão'."""
    t = texto.lower()
    prefixo = r'(?:acima\s+de|mais\s+de|superior\s+a|maior\s+que|a\s+partir\s+de|no\s+m[ií]nimo|m[ií]nimo\s+de)\s+'

    # 1 milhão / 2 milhões
    m_milhao = re.search(prefixo + r'(?:r\$\s*)?(\d+(?:[.,]\d+)?)\s*milh(?:ão|ões|ao|oes)', t)
    if m_milhao:
        val = float(m_milhao.group(1).replace(",", "."))
        return val * 1_000_000

    # 500k / 100k
    m_k = re.search(prefixo + r'(?:r\$\s*)?(\d+(?:[.,]\d+)?)\s*k\b', t)
    if m_k:
        val = float(m_k.group(1).replace(",", "."))
        return val * 1_000

    # 500 mil / 100 mil
    m_mil = re.search(prefixo + r'(?:r\$\s*)?(\d+(?:[.,]\d+)?)\s*mil\b', t)
    if m_mil:
        val = float(m_mil.group(1).replace(",", "."))
        return val * 1_000

    # R$ 500.000 / 100000
    m_num = re.search(prefixo + r'(?:r\$\s*)?(\d{1,3}(?:\.\d{3})+(?:,\d{2})?|\d{4,})', t)
    if m_num:
        limpo = m_num.group(1).replace(".", "").replace(",", ".")
        try:
            return float(limpo)
        except ValueError:
            pass

    return None


def _extrair_cidade_texto(texto: str) -> str:
    """Busca se alguma cidade relevante ou mencionada consta no texto."""
    # Lista de cidades mais buscadas no Brasil para matching rápido
    grandes_cidades = [
        "São Paulo", "Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba",
        "Porto Alegre", "Salvador", "Fortaleza", "Brasília", "Brasilia", "Recife",
        "Goiânia", "Goiania", "Belém", "Belem", "Manaus", "Campinas", "São Bernardo do Campo",
        "Santo André", "Osasco", "Sorocaba", "Ribeirão Preto", "Ribeirao Preto",
        "Santos", "São José dos Campos", "Sao Jose dos Campos", "Joinville", "Florianópolis",
        "Florianopolis", "Blumenau", "Caxias do Sul", "Londrina", "Maringá", "Maringa",
        "Uberlândia", "Uberlandia", "Contagem", "Juiz de Fora", "Niterói", "Niteroi",
        "Vila Velha", "Vitória", "Vitoria", "Campo Grande", "Cuiabá", "Cuiaba"
    ]
    t_lower = " " + texto.lower() + " "
    for c in grandes_cidades:
        pattern = r'\b' + re.escape(c.lower()) + r'\b'
        if re.search(pattern, t_lower):
            return c
    
    # Se tiver cache de municípios carregado, tenta localizar nomes de cidades precedidos de 'em', 'de', 'em cidade de'
    m_em = re.search(r'\b(?:em|de|na cidade de|no município de)\s+([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)', texto)
    if m_em:
        cand = m_em.group(1).strip()
        palavras_ignorar = {"Minas", "Gerais", "Paulo", "Janeiro", "Santa", "Catarina", "Rio", "Grande", "Sul", "Norte"}
        if cand not in palavras_ignorar and len(cand) > 3:
            return cand

    return ""


def parse_natural_language_query(query_text: str) -> dict:
    """
    Utiliza o TypeSafe AI (System One) para interpretar uma busca em linguagem natural
    e convertê-la em parâmetros tipados e determinísticos compatíveis com o banco de dados.
    """
    if not is_ai_configured():
        raise TypeSafeNotConfiguredError(
            "Chave TYPESAFE_API_KEY não configurada. Defina sua chave no arquivo .env para utilizar a busca inteligente."
        )

    texto_limpo = query_text.strip()
    if not texto_limpo:
        return {"filters": {}, "explanation": "Nenhum texto informado para consulta.", "confidence": 1.0}

    # Critérios de UF (todos os estados do Brasil + 'nenhuma')
    ufs_criteria = {uf: f"Estado de {uf}" for uf in LISTA_UFS}
    ufs_criteria["nenhuma"] = "Nenhuma UF informada na busca"

    # Critérios de Porte
    porte_criteria = {
        "2": "Microempresa (ME)",
        "3": "Empresa de Pequeno Porte (EPP)",
        "5": "Demais (Médio ou Grande Porte)",
        "qualquer": "Não especificado ou indiferente"
    }

    # Critérios de Situação Cadastral
    situacao_criteria = {
        "2": "Ativa (em atividade regular)",
        "8": "Baixada (encerrada)",
        "4": "Inapta",
        "3": "Suspensa",
        "qualquer": "Não especificado ou todas as situações"
    }

    # Executa consulta System One com julgamentos atômicos paralelos
    with TypeSafeClient(api_key=get_typesafe_api_key()) as client:
        response = client.system_one(
            state={
                "texto_consulta": texto_limpo,
                "data_hoje": datetime.now().strftime("%Y-%m-%d"),
            },
            questions={
                "uf": Choice(
                    instructions="Qual a Unidade Federativa (UF) brasileira solicitada pelo usuário no texto?",
                    criteria=ufs_criteria,
                ),
                "porte": Choice(
                    instructions="Qual o porte de empresa solicitado pelo usuário?",
                    criteria=porte_criteria,
                ),
                "situacao": Choice(
                    instructions="Qual a situação cadastral das empresas solicitada?",
                    criteria=situacao_criteria,
                ),
                "simples": Choice(
                    instructions="O usuário restringe a busca por empresas optantes pelo Simples Nacional?",
                    criteria={
                        "SIM": "Apenas optantes pelo Simples Nacional",
                        "NAO": "Apenas NÃO optantes pelo Simples",
                        "qualquer": "Indiferente / não informado",
                    },
                ),
                "mei": Choice(
                    instructions="O usuário restringe a busca especificamente para MEI (Microempreendedor Individual)?",
                    criteria={
                        "SIM": "Apenas MEIs",
                        "NAO": "Apenas não-MEIs",
                        "qualquer": "Indiferente / não informado",
                    },
                ),
                "matriz_filial": Choice(
                    instructions="O usuário quer filtrar apenas Matriz ou apenas Filiais?",
                    criteria={
                        "1": "Apenas Matriz",
                        "2": "Apenas Filiais",
                        "qualquer": "Indiferente / ambas",
                    },
                ),
                "busca_recente": Noul(
                    instructions="O usuário pediu empresas abertas recentemente, novas, ou criadas nos últimos anos?"
                ),
            },
        )

    # Mapeamento seguro dos resultados para os filtros da aplicação
    filtros = {}
    explicacoes = []
    confiancas = []

    # UF
    uf_escolhida = response.choices["uf"].choice
    if uf_escolhida and uf_escolhida != "nenhuma":
        filtros["uf"] = uf_escolhida
        explicacoes.append(f"Estado (UF): {uf_escolhida}")
        confiancas.append(response.choices["uf"].confidence)

    # Porte
    porte_escolhido = response.choices["porte"].choice
    if porte_escolhido and porte_escolhido != "qualquer":
        filtros["porte_empresa"] = porte_escolhido
        nome_porte = porte_criteria.get(porte_escolhido, porte_escolhido)
        explicacoes.append(f"Porte: {nome_porte}")
        confiancas.append(response.choices["porte"].confidence)

    # Situação Cadastral
    situacao_escolhida = response.choices["situacao"].choice
    if situacao_escolhida and situacao_escolhida != "qualquer":
        filtros["situacao_cadastral"] = situacao_escolhida
        nome_sit = situacao_criteria.get(situacao_escolhida, situacao_escolhida)
        explicacoes.append(f"Situação: {nome_sit}")
        confiancas.append(response.choices["situacao"].confidence)

    # Simples Nacional
    simples_escolhido = response.choices["simples"].choice
    if simples_escolhido and simples_escolhido != "qualquer":
        filtros["opcao_simples"] = simples_escolhido
        explicacoes.append(f"Simples Nacional: {simples_escolhido}")
        confiancas.append(response.choices["simples"].confidence)

    # MEI
    mei_escolhido = response.choices["mei"].choice
    if mei_escolhido and mei_escolhido != "qualquer":
        filtros["opcao_mei"] = mei_escolhido
        explicacoes.append(f"MEI: {mei_escolhido}")
        confiancas.append(response.choices["mei"].confidence)

    # Matriz / Filial
    matriz_escolhida = response.choices["matriz_filial"].choice
    if matriz_escolhida and matriz_escolhida != "qualquer":
        filtros["matriz_filial"] = matriz_escolhida
        tipo_lbl = "Matriz" if matriz_escolhida == "1" else "Filial"
        explicacoes.append(f"Tipo: {tipo_lbl}")
        confiancas.append(response.choices["matriz_filial"].confidence)

    # Data de Início Recente (se solicitado)
    if response.nouls["busca_recente"].noul > 0.7:
        ano_atual = datetime.now().year
        ano_corte = str(ano_atual - 2) + "0101"
        filtros["data_inicio_de"] = ano_corte
        explicacoes.append(f"Abertas a partir de {ano_atual - 2}")

    # Extrações Determinísticas Auxiliares (cidade, capital, palavras-chave)
    cidade = _extrair_cidade_texto(texto_limpo)
    if cidade:
        filtros["municipio"] = cidade
        explicacoes.append(f"Município: {cidade}")

    cap_min = _extrair_valor_monetario_texto(texto_limpo)
    if cap_min is not None:
        filtros["capital_min"] = str(int(cap_min))
        explicacoes.append(f"Capital Social Mínimo: R$ {cap_min:,.2f}")

    # Termos de atividade para filtro CNAE ou Razão Social
    termos_atividade = _extrair_termo_atividade(texto_limpo, uf_escolhida, cidade)
    if termos_atividade:
        filtros["cnae"] = termos_atividade
        explicacoes.append(f"Atividade/CNAE: '{termos_atividade}'")

    confianca_media = (sum(confiancas) / len(confiancas)) if confiancas else 0.95

    return {
        "filters": filtros,
        "explanation": " • ".join(explicacoes) if explicacoes else "Filtros interpretados pela IA",
        "confidence": round(confianca_media, 2),
    }


def _extrair_termo_atividade(texto: str, uf: str = "", cidade: str = "") -> str:
    """Extrai palavras representativas da atividade econômica ou nicho do texto da busca."""
    stop_words = {
        "de", "em", "para", "com", "sem", "ou", "e", "da", "do", "das", "dos",
        "no", "na", "nos", "nas", "por", "que", "empresa", "empresas", "ativas",
        "ativa", "baixadas", "abertas", "novas", "antigas", "recentes", "pequeno",
        "medio", "grande", "porte", "microempresa", "epp", "me", "simples", "nacional",
        "mei", "matriz", "filial", "filiais", "capital", "social", "acima", "mais",
        "menos", "minimo", "ano", "anos", "ultimos", "últimos", "procura", "buscar",
        "pesquisar", "encontrar", "listar"
    }
    if uf:
        stop_words.add(uf.lower())
    if cidade:
        for p in cidade.lower().split():
            stop_words.add(p)

    tokens = re.findall(r'\b[a-zA-ZÀ-ÿ0-9]+\b', texto.lower())
    relevantes = [t for t in tokens if t not in stop_words and len(t) > 2 and not t.isdigit()]
    
    return " ".join(relevantes[:3]) if relevantes else ""


def diagnosticar_empresa_cadastral(empresa_detalhes: dict) -> dict:
    """
    Executa uma análise semântica de conformidade cadastral, risco e governança
    utilizando julgamentos calibrados do TypeSafe AI (System One).
    """
    if not is_ai_configured():
        raise TypeSafeNotConfiguredError(
            "Chave TYPESAFE_API_KEY não configurada. Defina sua chave no arquivo .env para executar o diagnóstico."
        )

    emp = empresa_detalhes.get("empresa", {})
    mat = empresa_detalhes.get("matriz", {}) or {}
    socios = empresa_detalhes.get("socios", [])

    estado_empresa = {
        "razao_social": emp.get("razao_social", ""),
        "nome_fantasia": mat.get("nome_fantasia", "") or "Não informado",
        "capital_social": emp.get("capital_social", 0.0),
        "porte": emp.get("porte_descricao", "") or "Não informado",
        "natureza_juridica": emp.get("natureza_juridica_descricao", "") or "Não informada",
        "situacao_cadastral": mat.get("situacao_cadastral_descricao", "") or "Não informada",
        "data_inicio_atividade": mat.get("data_inicio", "") or "Não informada",
        "cnae_principal": mat.get("cnae_fiscal_principal_descricao", "") or "Não informado",
        "quantidade_socios": len(socios),
        "nomes_socios": [s.get("nome_socio", "") for s in socios[:5]],
        "simples_nacional": empresa_detalhes.get("simples_nacional", {}).get("opcao_simples", "NÃO"),
    }

    with TypeSafeClient(api_key=get_typesafe_api_key()) as client:
        response = client.system_one(
            state=estado_empresa,
            questions={
                "anomalia_capital": Noul(
                    instructions=(
                        "Avalie se há incoerência grave ou discrepância desproporcional entre "
                        "o capital social informado e o porte/atividade declarada da empresa."
                    )
                ),
                "conflito_nome_atividade": Noul(
                    instructions=(
                        "O nome empresarial (razão social ou fantasia) está em conflito flagrante "
                        "ou diverge totalmente da atividade econômica principal (CNAE) registrada?"
                    )
                ),
                "complexidade_societaria": Score(
                    instructions="Avalie o nível de complexidade da composição societária e governança da empresa.",
                    criteria=[
                        "Simples (sociedade unipessoal ou sócios pessoas físicas comuns)",
                        "Moderada (múltiplos sócios e administração compartilhada)",
                        "Complexa (estrutura com holdings, pessoas jurídicas ou múltiplos administradores)",
                    ],
                ),
                "perfil_operacional": Choice(
                    instructions="Qual é o perfil operacional predominante desta empresa com base no CNAE e dados cadastrais?",
                    criteria={
                        "b2b": "B2B Corporativo (serviços empresariais, tecnologia, atacado, consultoria)",
                        "b2c": "B2C Varejo / Consumidor Final (comércio lojista, alimentação, serviços pessoais)",
                        "industria": "Indústria / Produção / Agronegócio / Construção",
                        "patrimonial": "Holding / Administração Patrimonial / Gestão de Ativos",
                        "outros": "Outros ou perfil cadastral genérico",
                    },
                ),
            },
        )

    prob_anomalia_capital = response.nouls["anomalia_capital"].noul
    prob_conflito_nome = response.nouls["conflito_nome_atividade"].noul
    score_val = response.scores["complexidade_societaria"].score
    try:
        score_num = float(score_val)
        idx = min(max(round(score_num), 0), 2)
        nomes_complexidade = ["Simples", "Moderada", "Complexa"]
        descricao_complexidade = nomes_complexidade[idx]
    except Exception:
        descricao_complexidade = str(score_val)

    perfil_escolhido = str(response.choices["perfil_operacional"].choice or "outros")
    confianca_perfil = float(response.choices["perfil_operacional"].confidence or 0.95)

    # Cálculo da classificação de risco cadastral em código
    if prob_anomalia_capital > 0.7 or prob_conflito_nome > 0.7:
        nivel_risco = "Atenção"
        cor_risco = "red"
        status_resumo = "Inconsistências ou discrepâncias cadastrais detectadas"
    elif prob_anomalia_capital > 0.35 or prob_conflito_nome > 0.35:
        nivel_risco = "Moderado"
        cor_risco = "amber"
        status_resumo = "Dados cadastrais comuns com pontos de atenção leves"
    else:
        nivel_risco = "Baixo"
        cor_risco = "emerald"
        status_resumo = "Cadastro consistente e compatível com as atividades"

    mapa_perfil = {
        "b2b": "B2B Corporativo",
        "b2c": "B2C Varejo & Serviços",
        "industria": "Indústria & Produção",
        "patrimonial": "Holding & Patrimonial",
        "outros": "Operacional Geral",
    }

    observacoes = []
    if prob_anomalia_capital > 0.6:
        observacoes.append("Capital social parece desproporcional ao porte ou atividade descrita.")
    else:
        observacoes.append("Capital social alinhado ao enquadramento cadastral declarado.")

    if prob_conflito_nome > 0.6:
        observacoes.append("Divergência entre o nome empresarial e o CNAE fiscal principal.")
    else:
        observacoes.append("Nome empresarial condizente com as atividades econômicas informadas.")

    observacoes.append(f"Estrutura societária avaliada como {descricao_complexidade.lower()}.")

    return {
        "nivel_risco": nivel_risco,
        "cor_risco": cor_risco,
        "status_resumo": status_resumo,
        "perfil_operacional": mapa_perfil.get(perfil_escolhido, perfil_escolhido),
        "complexidade_societaria": descricao_complexidade,
        "probabilidade_anomalia_capital": round(prob_anomalia_capital, 2),
        "probabilidade_conflito_nome": round(prob_conflito_nome, 2),
        "confianca_analise": round(confianca_perfil, 2),
        "observacoes": observacoes,
    }
