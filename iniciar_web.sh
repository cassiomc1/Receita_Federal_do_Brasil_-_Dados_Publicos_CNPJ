#!/usr/bin/env bash
# ==================================================================================================
# Script: iniciar_web.sh
# Finalidade: Inicializar o servidor da interface web do CNPJ Explorer (Shadcn UI Minimalista)
#
# Uso:
#   ./iniciar_web.sh [--port 5000] [--host 0.0.0.0]
# ==================================================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ativa o ambiente virtual se existir
if [[ -f "${SCRIPT_DIR}/venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/venv/bin/activate"
fi

# Inicia o aplicativo Flask
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
python3 -m web.app "$@"
