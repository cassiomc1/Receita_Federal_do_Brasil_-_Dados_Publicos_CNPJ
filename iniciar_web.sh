#!/usr/bin/env bash
# ==================================================================================================
# Script: iniciar_web.sh
# Finalidade: Inicializar o servidor da interface web do CNPJ Explorer (Shadcn UI Minimalista)
#             Verifica se o firewall está ativo e adiciona a porta usada nas regras (Oracle Linux 9)
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

# Ativa o ambiente virtual se existir
if [[ -f "${SCRIPT_DIR}/venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/venv/bin/activate"
fi

# Inicia o aplicativo Flask
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
python3 -m web.app "$@"
