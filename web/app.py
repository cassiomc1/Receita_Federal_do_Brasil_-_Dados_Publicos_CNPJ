# -*- coding: utf-8 -*-
"""
Aplicação Web Principal do Painel de Consulta aos Dados Públicos de CNPJ.
Construído com Flask, visual inspirado no Shadcn UI (tema claro, minimalista),
autenticação com senha gerada a cada inicialização, consultas avançadas e exportação CSV.
"""

import argparse
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
        LISTA_UFS,
        MAPA_SITUACAO,
        MAPA_PORTE,
        MAPA_MATRIZ_FILIAL,
        CACHE_DOMINIOS,
        get_connection,
        release_connection,
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
        LISTA_UFS,
        MAPA_SITUACAO,
        MAPA_PORTE,
        MAPA_MATRIZ_FILIAL,
        CACHE_DOMINIOS,
        get_connection,
        release_connection,
        DB_NAME,
        DB_HOST,
    )

app = Flask(__name__)
app.secret_key = APP_SECRET_KEY

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
    """Exporta a listagem atual filtrada diretamente em formato CSV para download."""
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
    """Verifica a saúde da conexão com o banco de dados e conta registros aproximados."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM "empresa") AS total_empresa,
                    (SELECT COUNT(*) FROM "estabelecimento") AS total_est;
            """)
            emp_count, est_count = cur.fetchone()
            return jsonify({
                "status": "online",
                "database": DB_NAME,
                "host": DB_HOST,
                "total_empresas": emp_count,
                "total_estabelecimentos": est_count,
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
