# -*- coding: utf-8 -*-
"""
Módulo de Autenticação e Segurança do Painel Web CNPJ.
Gera novas credenciais de login a cada inicialização da aplicação,
exibindo-as de forma destacada no terminal.
"""

import functools
import secrets
from flask import session, redirect, url_for, request, jsonify

# Gera usuário padrão e senha aleatória de alta entropia a cada inicialização
DEFAULT_USERNAME = "admin"
RANDOM_PASSWORD = f"rfb-{secrets.token_hex(3)}"
APP_SECRET_KEY = secrets.token_hex(32)

def print_startup_credentials(host="0.0.0.0", port=5000):
    """Exibe no terminal um banner destacado com as credenciais geradas."""
    display_host = "localhost" if host == "0.0.0.0" else host
    print("\n" + "=" * 72)
    print(" ⚡ CNPJ EXPLORER - PAINEL DE CONSULTA (RECEITA FEDERAL DO BRASIL)")
    print("=" * 72)
    print(" Credenciais de acesso temporárias geradas para esta sessão:")
    print(f"   👤 Usuário: \033[1;32m{DEFAULT_USERNAME}\033[0m")
    print(f"   🔑 Senha:   \033[1;33m{RANDOM_PASSWORD}\033[0m")
    print("-" * 72)
    print(f" 🌐 Acesse no seu navegador: \033[1;36mhttp://{display_host}:{port}\033[0m")
    print(" ℹ️  Uma nova senha e chave secreta são geradas a cada reinício do servidor.")
    print("=" * 72 + "\n")

def check_credentials(username, password):
    """Verifica se o usuário e a senha fornecidos correspondem aos gerados."""
    if not username or not password:
        return False
    # Comparação em tempo constante para proteção contra timing attacks
    user_match = secrets.compare_digest(username.strip(), DEFAULT_USERNAME)
    pass_match = secrets.compare_digest(password.strip(), RANDOM_PASSWORD)
    return user_match and pass_match

def is_authenticated():
    """Verifica se o usuário atual está autenticado na sessão."""
    return session.get("authenticated") is True

def login_user(username):
    """Marca a sessão como autenticada."""
    session["authenticated"] = True
    session["username"] = username

def logout_user():
    """Encerra a sessão atual."""
    session.clear()

def login_required(f):
    """Decorator para proteger rotas web e endpoints de API."""
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_authenticated():
            if request.path.startswith("/api/"):
                return jsonify({"error": "Não autorizado. Efetue login para continuar."}), 401
            return redirect(url_for("login", next=request.url))
        return f(*args, **kwargs)
    return decorated_function
