# -*- coding: utf-8 -*-
"""
Aplicação Web Flask - Consulta aos Dados Públicos do CNPJ (RFB).
Interface moderna baseada no Shadcn UI com tema claro, tipografia Inter,
autenticação com senha gerada a cada inicialização, consultas avançadas e exportação CSV / XLS (Excel).
"""

import argparse
import os
import pathlib
import sys
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, Response

# Garante resolução de imports tanto via 'python web/app.py' quanto 'python -m web.app'
ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from web.auth import (
        APP_SECRET_KEY,
        DEFAULT_USERNAME,
        RANDOM_PASSWORD,
        login_required,
        login_user,
        logout_user,
        is_authenticated,
        check_credentials,
        print_startup_credentials,
    )
    from web.database import (
        search_empresas,
        get_empresa_details,
        generate_csv_stream,
        generate_excel_file,
        DependenciaAusenteError,
        LISTA_UFS,
        MAPA_SITUACAO,
        MAPA_PORTE,
        MAPA_MATRIZ_FILIAL,
        CACHE_DOMINIOS,
        get_connection,
        release_connection,
        get_rfb_metadata,
        DB_NAME,
        DB_HOST,
    )
except ImportError:
    from auth import (
        APP_SECRET_KEY,
        DEFAULT_USERNAME,
        RANDOM_PASSWORD,
        login_required,
        login_user,
        logout_user,
        is_authenticated,
        check_credentials,
        print_startup_credentials,
    )
    from database import (
        search_empresas,
        get_empresa_details,
        generate_csv_stream,
        generate_excel_file,
        DependenciaAusenteError,
        LISTA_UFS,
        MAPA_SITUACAO,
        MAPA_PORTE,
        MAPA_MATRIZ_FILIAL,
        CACHE_DOMINIOS,
        get_connection,
        release_connection,
        get_rfb_metadata,
        DB_NAME,
        DB_HOST,
    )

app = Flask(__name__)
app.secret_key = APP_SECRET_KEY

def _pagina_aviso_exportacao(mensagem):
    """Página HTML exibida no navegador quando uma exportação está indisponível no servidor."""
    from html import escape
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Exportação indisponível - CNPJ Explorer</title>
    <style>
        body {{ font-family: Inter, -apple-system, 'Segoe UI', sans-serif; background: #fafafa;
               color: #18181b; display: flex; align-items: center; justify-content: center;
               min-height: 100vh; margin: 0; padding: 24px; }}
        .card {{ background: #fff; border: 1px solid #e4e4e7; border-radius: 12px; padding: 28px;
                max-width: 620px; box-shadow: 0 1px 3px rgba(0,0,0,.06); }}
        h1 {{ font-size: 17px; margin: 0 0 10px; }}
        p {{ font-size: 13px; line-height: 1.6; color: #3f3f46; margin: 0 0 14px; }}
        code {{ background: #f4f4f5; border: 1px solid #e4e4e7; border-radius: 5px;
                padding: 2px 6px; font-size: 12px; }}
        pre {{ background: #f4f4f5; border: 1px solid #e4e4e7; border-radius: 8px; padding: 12px;
               font-size: 12px; overflow-x: auto; margin: 0 0 16px; }}
        a {{ display: inline-block; background: #18181b; color: #fff; text-decoration: none;
             padding: 9px 16px; border-radius: 7px; font-size: 13px; font-weight: 500; }}
        .tag {{ display: inline-block; font-size: 11px; font-weight: 600; color: #b45309;
                background: #fffbeb; border: 1px solid #fde68a; border-radius: 999px;
                padding: 2px 10px; margin-bottom: 12px; }}
    </style>
</head>
<body>
    <div class="card">
        <span class="tag">Exportação XLS indisponível</span>
        <h1>O servidor não consegue gerar a planilha Excel</h1>
        <p>{escape(mensagem)}</p>
        <p>No servidor, execute:</p>
        <pre>pip install -r requirements.txt
# ou apenas o pacote necessário:
pip install openpyxl</pre>
        <p>Depois reinicie o servidor. Enquanto isso, use a exportação <strong>CSV</strong>, que não
           depende desse pacote.</p>
        <a href="/">Voltar para a consulta</a>
    </div>
</body>
</html>"""

# --------------------------------------------------------------------------------------------------
# Rotas de Autenticação e Páginas
# --------------------------------------------------------------------------------------------------
@app.route("/")
@login_required
def index():
    return render_template("index.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    if is_authenticated():
        return redirect(url_for("index"))

    error_message = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if check_credentials(username, password):
            login_user(username)
            next_url = request.args.get("next") or url_for("index")
            return redirect(next_url)
        else:
            error_message = "Credenciais inválidas. Verifique a senha temporária exibida no terminal."

    return render_template(
        "login.html",
        error_message=error_message,
        hint_user=DEFAULT_USERNAME,
    )

@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for("login"))

# --------------------------------------------------------------------------------------------------
# Endpoints de API REST
# --------------------------------------------------------------------------------------------------
@app.route("/api/empresas", methods=["GET"])
@login_required
def api_empresas():
    """Consulta empresas com base nos parâmetros da URL e retorna resultado paginado."""
    filters = {
        "cnpj": request.args.get("cnpj", ""),
        "razao_social": request.args.get("razao_social", ""),
        "nome_fantasia": request.args.get("nome_fantasia", ""),
        "uf": request.args.get("uf", ""),
        "municipio": request.args.get("municipio", ""),
        "situacao_cadastral": request.args.get("situacao_cadastral", ""),
        "matriz_filial": request.args.get("matriz_filial", ""),
        "porte_empresa": request.args.get("porte_empresa", ""),
        "cnae": request.args.get("cnae", ""),
        "natureza_juridica": request.args.get("natureza_juridica", ""),
        "opcao_simples": request.args.get("opcao_simples", ""),
        "opcao_mei": request.args.get("opcao_mei", ""),
        "capital_min": request.args.get("capital_min", ""),
        "capital_max": request.args.get("capital_max", ""),
        "data_inicio_de": request.args.get("data_inicio_de", ""),
        "data_inicio_ate": request.args.get("data_inicio_ate", ""),
        "ordenar_por": request.args.get("ordenar_por", ""),
        "limite": request.args.get("limite", ""),
    }

    try:
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 25))
    except ValueError:
        page = 1
        page_size = 25

    response_data = search_empresas(filters, page=page, page_size=page_size)
    return jsonify(response_data)

@app.route("/api/empresa/<cnpj_basico>", methods=["GET"])
@login_required
def api_empresa_detalhes(cnpj_basico):
    """Retorna a ficha cadastral completa da empresa pelo CNPJ básico."""
    detalhes = get_empresa_details(cnpj_basico)
    if not detalhes:
        return jsonify({"error": "Empresa não encontrada"}), 404
    return jsonify(detalhes)

@app.route("/api/exportar-csv", methods=["GET"])
@login_required
def api_exportar_csv():
    """Exporta o TOTAL filtrado diretamente em formato CSV para download.

    Se "Limitar Resultados" = 1000, exporta até 1000 linhas; sem limite, até 10000.
    """
    filters = {
        "cnpj": request.args.get("cnpj", ""),
        "razao_social": request.args.get("razao_social", ""),
        "nome_fantasia": request.args.get("nome_fantasia", ""),
        "uf": request.args.get("uf", ""),
        "municipio": request.args.get("municipio", ""),
        "situacao_cadastral": request.args.get("situacao_cadastral", ""),
        "matriz_filial": request.args.get("matriz_filial", ""),
        "porte_empresa": request.args.get("porte_empresa", ""),
        "cnae": request.args.get("cnae", ""),
        "natureza_juridica": request.args.get("natureza_juridica", ""),
        "opcao_simples": request.args.get("opcao_simples", ""),
        "opcao_mei": request.args.get("opcao_mei", ""),
        "capital_min": request.args.get("capital_min", ""),
        "capital_max": request.args.get("capital_max", ""),
        "data_inicio_de": request.args.get("data_inicio_de", ""),
        "data_inicio_ate": request.args.get("data_inicio_ate", ""),
        "ordenar_por": request.args.get("ordenar_por", ""),
        "limite": request.args.get("limite", ""),
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"empresas_rfb_export_{timestamp}.csv"

    return Response(
        generate_csv_stream(filters, max_rows=10000),
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache",
        },
    )

@app.route("/api/exportar-xls", methods=["GET"])
@app.route("/api/exportar-xlsx", methods=["GET"])
@app.route("/api/exportar-excel", methods=["GET"])
@login_required
def api_exportar_xls():
    """Exporta o TOTAL filtrado diretamente em planilha Excel (XLS/XLSX) para download.

    Se "Limitar Resultados" = 1000, exporta até 1000 linhas; sem limite, até 10000.
    """
    filters = {
        "cnpj": request.args.get("cnpj", ""),
        "razao_social": request.args.get("razao_social", ""),
        "nome_fantasia": request.args.get("nome_fantasia", ""),
        "uf": request.args.get("uf", ""),
        "municipio": request.args.get("municipio", ""),
        "situacao_cadastral": request.args.get("situacao_cadastral", ""),
        "matriz_filial": request.args.get("matriz_filial", ""),
        "porte_empresa": request.args.get("porte_empresa", ""),
        "cnae": request.args.get("cnae", ""),
        "natureza_juridica": request.args.get("natureza_juridica", ""),
        "opcao_simples": request.args.get("opcao_simples", ""),
        "opcao_mei": request.args.get("opcao_mei", ""),
        "capital_min": request.args.get("capital_min", ""),
        "capital_max": request.args.get("capital_max", ""),
        "data_inicio_de": request.args.get("data_inicio_de", ""),
        "data_inicio_ate": request.args.get("data_inicio_ate", ""),
        "ordenar_por": request.args.get("ordenar_por", ""),
        "limite": request.args.get("limite", ""),
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = request.args.get("ext", "xlsx").lower()
    if ext not in ("xlsx", "xls"):
        ext = "xlsx"
    filename = f"empresas_rfb_export_{timestamp}.{ext}"

    try:
        excel_io = generate_excel_file(filters, max_rows=10000)
    except DependenciaAusenteError as e:
        # Sem o openpyxl a exportação XLS fica indisponível: devolve uma página explicativa
        # (o download é aberto pelo navegador) em vez de um erro 500 com traceback.
        return Response(_pagina_aviso_exportacao(str(e)), status=503, mimetype="text/html; charset=utf-8")

    mimetype = (
        "application/vnd.ms-excel"
        if ext == "xls"
        else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    return Response(
        excel_io.getvalue(),
        mimetype=mimetype,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache",
        },
    )

@app.route("/api/dominios", methods=["GET"])
@login_required
def api_dominios():
    """Retorna os dados dos domínios para preencher os selects de filtros."""
    natju_lista = sorted(
        [{"codigo": k, "descricao": f"{k} - {v}"} for k, v in CACHE_DOMINIOS["natju"].items()],
        key=lambda x: x["codigo"]
    )
    return jsonify({
        "ufs": LISTA_UFS,
        "situacoes": [{"codigo": k, "descricao": v} for k, v in MAPA_SITUACAO.items()],
        "portes": [{"codigo": k, "descricao": v} for k, v in MAPA_PORTE.items()],
        "tipos": [{"codigo": k, "descricao": v} for k, v in MAPA_MATRIZ_FILIAL.items()],
        "naturezas_juridicas": natju_lista,
    })

@app.route("/api/status", methods=["GET"])
@login_required
def api_status():
    """Verifica a saúde da conexão com o banco e retorna a versão/totais já registrados.

    Não executa COUNT(*) nas tabelas de estabelecimentos/empresas: em ~55 milhões de linhas
    cada contagem exige varredura completa e tornava a abertura da página pesada. Os totais
    são lidos da tabela de metadados "_metadados_rfb", preenchida pelo ETL ao final da carga.
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
            meta_rfb = get_rfb_metadata()
            return jsonify({
                "status": "online",
                "database": DB_NAME,
                "host": DB_HOST,
                "total_empresas": (meta_rfb or {}).get("total_empresas"),
                "total_estabelecimentos": (meta_rfb or {}).get("total_estabelecimentos"),
                "versao_rfb": meta_rfb,
            })
    except Exception as e:
        return jsonify({
            "status": "erro",
            "database": DB_NAME,
            "host": DB_HOST,
            "mensagem": str(e),
        }), 500
    finally:
        if conn:
            release_connection(conn)

@app.route("/api/verificar-atualizacao", methods=["GET"])
@login_required
def api_verificar_atualizacao():
    """Consulta o WebDAV oficial da Receita Federal e verifica se há nova base disponível."""
    import requests
    from xml.etree import ElementTree
    import re

    meta_banco = get_rfb_metadata()
    mes_banco = meta_banco["ano_mes"] if meta_banco else None

    share_token = os.getenv("RFB_SHARE_TOKEN", "YggdBLfdninEJX9")
    webdav_base_url = os.getenv("RFB_WEBDAV_URL", "https://arquivos.receitafederal.gov.br/public.php/webdav")

    try:
        DAV_NS = {"d": "DAV:"}
        url = webdav_base_url.rstrip("/") + "/"
        headers = {"Depth": "1"}
        resp = requests.request("PROPFIND", url, auth=(share_token, ""), headers=headers, verify=False, timeout=15)
        resp.raise_for_status()
        root = ElementTree.fromstring(resp.content)

        directories = []
        for r in root.findall("d:response", DAV_NS):
            href = r.find("d:href", DAV_NS).text
            match = re.search(r"(\d{4}-\d{2})/?$", href)
            if match:
                directories.append(match.group(1))
        directories.sort()

        if not directories:
            return jsonify({"status": "erro", "mensagem": "Nenhuma pasta mensal localizada na RFB"}), 502

        mes_remoto = directories[-1]
        tem_atualizacao = bool(not mes_banco or mes_banco != mes_remoto)

        return jsonify({
            "status": "sucesso",
            "mes_remoto": mes_remoto,
            "mes_banco": mes_banco,
            "tem_atualizacao": tem_atualizacao,
            "metadados_banco": meta_banco,
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": f"Falha ao conectar com o servidor da RFB: {e}"}), 502

# --------------------------------------------------------------------------------------------------
# Ponto de Entrada da Execução
# --------------------------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Servidor Web - Painel CNPJ Receita Federal")
    parser.add_argument("--host", default="0.0.0.0", help="Endereço de escuta do servidor (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="Porta HTTP (padrão: 5000)")
    args = parser.parse_args()

    print_startup_credentials(host=args.host, port=args.port)
    app.run(host=args.host, port=args.port, debug=False)

if __name__ == "__main__":
    main()
