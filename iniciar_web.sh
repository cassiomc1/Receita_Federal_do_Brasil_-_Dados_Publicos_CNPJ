#!/usr/bin/env bash
# ==================================================================================================
# Script: iniciar_web.sh
# Finalidade: Inicializar o servidor da interface web do CNPJ Explorer (Shadcn UI Minimalista)
#             Verifica e libera o firewall automaticamente (Oracle Linux 9)
#             Verifica e instala automaticamente dependências faltantes (Flask, etc.)
#
# Uso:
#   ./iniciar_web.sh [--port 5000] [--host 0.0.0.0]
# ==================================================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Cores e Formatação
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCESSO]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[AVISO]${NC} $1"; }
log_error()   { echo -e "${RED}[ERRO]${NC} $1"; }

# --------------------------------------------------------------------------------------------------
# Identifica a porta a ser utilizada (padrão: 5000)
# --------------------------------------------------------------------------------------------------
PORT=5000
for ((i=1; i<=$#; i++)); do
    arg="${!i}"
    case "$arg" in
        --port=*)
            PORT="${arg#*=}"
            ;;
        --port|-p)
            next_idx=$((i+1))
            if [[ $next_idx -le $# ]]; then
                PORT="${!next_idx}"
            fi
            ;;
    esac
done

# --------------------------------------------------------------------------------------------------
# Verificação e Liberação de Firewall (Oracle Linux 9 / RHEL / OCI)
# --------------------------------------------------------------------------------------------------
verificar_e_liberar_firewall() {
    local target_port="$1"
    local sudo_cmd=""

    if [[ $EUID -ne 0 ]]; then
        if command -v sudo &>/dev/null; then
            sudo_cmd="sudo"
        fi
    fi

    log_info "Verificando status do firewall para a porta ${target_port}/tcp..."

    local rule_configured=false
    local firewalld_active=false

    # 1. Verifica firewalld (gerenciador padrão no Oracle Linux 9 / RHEL 9)
    if command -v firewall-cmd &>/dev/null; then
        if command -v systemctl &>/dev/null && systemctl is-active --quiet firewalld 2>/dev/null; then
            firewalld_active=true
        elif firewall-cmd --state &>/dev/null; then
            firewalld_active=true
        fi
    fi

    if [[ "$firewalld_active" = true ]]; then
        log_info "Firewalld ativo detectado no sistema."
        if $sudo_cmd firewall-cmd --query-port="${target_port}/tcp" &>/dev/null; then
            log_success "Porta ${target_port}/tcp já está liberada nas regras do firewalld."
            rule_configured=true
        else
            log_info "Liberando tráfego para a porta ${target_port}/tcp nas regras do firewalld..."
            if $sudo_cmd firewall-cmd --permanent --add-port="${target_port}/tcp" &>/dev/null && \
               $sudo_cmd firewall-cmd --reload &>/dev/null; then
                log_success "Porta ${target_port}/tcp liberada com sucesso no firewalld (--permanent)!"
                rule_configured=true
            else
                log_warn "Aviso: Não foi possível adicionar a regra automaticamente no firewalld (requer permissão sudo/root)."
            fi
        fi
    fi

    # 2. Verifica iptables (comum em instâncias OCI / Oracle Cloud Infrastructure)
    if command -v iptables &>/dev/null; then
        local has_drop_rules=false
        if $sudo_cmd iptables -S INPUT 2>/dev/null | grep -qE "(DROP|REJECT)"; then
            has_drop_rules=true
        fi

        if [[ "$has_drop_rules" = true ]]; then
            if ! $sudo_cmd iptables -C INPUT -p tcp --dport "${target_port}" -j ACCEPT &>/dev/null; then
                log_info "Regras restritivas detectadas no iptables (Oracle Cloud). Inserindo regra de liberação..."
                if $sudo_cmd iptables -I INPUT 1 -p tcp --dport "${target_port}" -j ACCEPT &>/dev/null; then
                    log_success "Porta ${target_port}/tcp liberada com sucesso no iptables!"
                    # Salva permanentemente caso o arquivo de persistência do iptables exista
                    if [[ -f /etc/sysconfig/iptables ]] && command -v iptables-save &>/dev/null; then
                        $sudo_cmd sh -c "iptables-save > /etc/sysconfig/iptables" 2>/dev/null || true
                    fi
                    rule_configured=true
                fi
            else
                log_success "Porta ${target_port}/tcp já está liberada no iptables."
                rule_configured=true
            fi
        fi
    fi

    if [[ "$rule_configured" = false && "$firewalld_active" = false ]]; then
        log_info "Firewall local inativo ou não restritivo para a porta ${target_port}/tcp."
    fi
}

# Executa verificação e liberação do firewall
verificar_e_liberar_firewall "$PORT"

# --------------------------------------------------------------------------------------------------
# Preparação do Ambiente Python e Dependências Web
# --------------------------------------------------------------------------------------------------
# Ativa o ambiente virtual se existir em locais conhecidos
if [[ -f "${SCRIPT_DIR}/venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/venv/bin/activate"
elif [[ -f "/home/opc/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ/venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "/home/opc/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ/venv/bin/activate"
fi

# Detecta o interpretador Python 3 e, se não houver nenhum, tenta instalar via DNF.
# Sem esta etapa o script morria com "No such file or directory" antes de qualquer verificação.
detecta_python() {
    if command -v python3 &>/dev/null; then
        PYTHON_EXEC="python3"
    elif command -v python3.11 &>/dev/null; then
        PYTHON_EXEC="python3.11"
    else
        PYTHON_EXEC=""
    fi
}

instala_python() {
    command -v dnf &>/dev/null || return 0
    log_warn "Nenhum interpretador Python 3 encontrado no sistema."
    log_info "Instalando Python 3 e pip via DNF (Oracle Linux 9)..."
    if [[ $EUID -eq 0 ]]; then
        dnf install -y python3 python3-pip 2>/dev/null || true
    elif command -v sudo &>/dev/null; then
        sudo dnf install -y python3 python3-pip 2>/dev/null || true
    fi
}

PYTHON_EXEC=""
detecta_python
if [[ -z "$PYTHON_EXEC" ]]; then
    instala_python
    detecta_python
fi

if [[ -z "$PYTHON_EXEC" ]]; then
    log_error "Python 3 não está instalado e não foi possível instalá-lo automaticamente."
    log_info "Instale manualmente e execute novamente: sudo dnf install -y python3 python3-pip"
    exit 1
fi

# Módulos exigidos pelo servidor web (sem eles a aplicação não inicia)
DEPS_OBRIGATORIAS="flask psycopg2 dotenv"
# Módulos opcionais: a ausência não impede o servidor de subir, apenas desabilita um recurso
DEPS_OPCIONAIS="openpyxl requests"

# Imprime os módulos ausentes dentre os nomes informados (vazio se todos estiverem instalados).
# Se o interpretador não puder ser executado, considera todos ausentes em vez de abortar o script.
modulos_ausentes() {
    $PYTHON_EXEC - "$@" <<'PY' 2>/dev/null || echo "$*"
import importlib.util
import sys
print(" ".join(nome for nome in sys.argv[1:] if importlib.util.find_spec(nome) is None))
PY
}

# Garante que os módulos Flask e demais dependências web estejam instalados.
# A verificação é feita módulo a módulo: checar apenas o Flask deixava dependências como
# o openpyxl sem instalar quando o Flask já existia no ambiente.
FALTANDO="$(modulos_ausentes $DEPS_OBRIGATORIAS $DEPS_OPCIONAIS)"

if [[ -n "$FALTANDO" ]]; then
    log_warn "Dependências web ausentes no ambiente Python atual ($($PYTHON_EXEC -V 2>&1)): ${FALTANDO}"
    log_info "Instalando dependências web necessárias..."

    INSTALLED=false
    ULTIMO_ERRO=""

    # Instala o requirements.txt quando disponível; senão, os pacotes individualmente
    if [[ -f "${SCRIPT_DIR}/requirements.txt" ]]; then
        PACOTES=(-r "${SCRIPT_DIR}/requirements.txt")
    else
        PACOTES=(flask psycopg2-binary SQLAlchemy python-dotenv openpyxl requests)
    fi

    # A ordem importa: prioriza o pip do próprio interpretador que executará a aplicação.
    # Usar um 'pip' genérico do PATH pode instalar em OUTRO Python e deixar a aplicação sem o
    # módulo — foi a causa do erro "No module named 'openpyxl'" com o pacote já "instalado".
    if [[ -x "${SCRIPT_DIR}/venv/bin/pip" ]]; then
        ULTIMO_ERRO="$("${SCRIPT_DIR}/venv/bin/pip" install "${PACOTES[@]}" --quiet 2>&1)" && INSTALLED=true || true
    fi

    if [ "$INSTALLED" = false ]; then
        ULTIMO_ERRO="$($PYTHON_EXEC -m pip install "${PACOTES[@]}" --quiet 2>&1)" && INSTALLED=true || true
    fi

    if [ "$INSTALLED" = false ]; then
        if command -v pip &>/dev/null; then
            ULTIMO_ERRO="$(pip install "${PACOTES[@]}" --quiet 2>&1)" && INSTALLED=true || \
            ULTIMO_ERRO="$(pip install "${PACOTES[@]}" --break-system-packages --quiet 2>&1)" && INSTALLED=true || true
        elif command -v pip3 &>/dev/null; then
            ULTIMO_ERRO="$(pip3 install "${PACOTES[@]}" --quiet 2>&1)" && INSTALLED=true || \
            ULTIMO_ERRO="$(pip3 install "${PACOTES[@]}" --break-system-packages --quiet 2>&1)" && INSTALLED=true || true
        fi
    fi

    # Fallback DNF no Oracle Linux 9 se o pip não estava disponível
    if [ "$INSTALLED" = false ] && ! $PYTHON_EXEC -m pip --version &>/dev/null && command -v dnf &>/dev/null; then
        log_info "Tentando instalação via gerenciador de pacotes DNF (Oracle Linux 9)..."
        if [[ $EUID -eq 0 ]]; then
            dnf install -y python3-pip python3-flask 2>/dev/null || true
        elif command -v sudo &>/dev/null; then
            sudo dnf install -y python3-pip python3-flask 2>/dev/null || true
        fi
        ULTIMO_ERRO="$($PYTHON_EXEC -m pip install "${PACOTES[@]}" --quiet 2>&1)" && INSTALLED=true || true
    fi

    FALTANDO_OBRIGATORIAS="$(modulos_ausentes $DEPS_OBRIGATORIAS)"
    if [[ -n "$FALTANDO_OBRIGATORIAS" ]]; then
        log_error "Não foi possível instalar os módulos obrigatórios: ${FALTANDO_OBRIGATORIAS}"
        if [[ -n "$ULTIMO_ERRO" ]]; then
            log_info "Última saída do pip:"
            echo "$ULTIMO_ERRO" | tail -5
        fi
        log_info "Execute manualmente: $PYTHON_EXEC -m pip install -r requirements.txt"
        exit 1
    fi

    # Módulos opcionais ausentes não impedem o servidor de subir: apenas avisam qual recurso fica indisponível
    if [[ -n "$(modulos_ausentes openpyxl)" ]]; then
        log_warn "Módulo 'openpyxl' não instalado: a exportação para Excel (XLS) ficará indisponível."
        log_info "Para habilitar: $PYTHON_EXEC -m pip install openpyxl"
    fi
    if [[ -n "$(modulos_ausentes requests)" ]]; then
        log_warn "Módulo 'requests' não instalado: a verificação de nova base da RFB ficará indisponível."
        log_info "Para habilitar: $PYTHON_EXEC -m pip install requests"
    fi

    log_success "Dependências web verificadas com sucesso!"
fi

# Inicia o aplicativo Flask
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
exec $PYTHON_EXEC -m web.app "$@"
