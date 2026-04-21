#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/venv"
RECREATE_VENV="${RECREATE_VENV:-1}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

print_section() {
  printf "\n=================================================================\n"
  printf "%s\n" "$1"
  printf "=================================================================\n"
}

create_project_env() {
  if [[ "${RECREATE_VENV}" != "0" && -d "${VENV_DIR}" ]]; then
    rm -rf "${VENV_DIR}"
  fi

  if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
    uv venv --python "${PYTHON_BIN}" "${VENV_DIR}"
  fi
}

install_python_deps() {
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  uv sync --active --no-editable \
    --refresh-package fingerprint-orchestrator \
    --reinstall-package fingerprint-orchestrator
}

run_sanity_checks() {
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"

  print_section "Running runtime sanity checks"
  python - <<'PY'
import importlib
checks = [
    ("FastAPI", "fastapi"),
    ("Uvicorn", "uvicorn"),
    ("aiomqtt", "aiomqtt"),
    ("MinIO", "minio"),
    ("asyncpg", "asyncpg"),
    ("Pydantic", "pydantic"),
    ("pydantic-settings", "pydantic_settings"),
]
for label, module_name in checks:
    module = importlib.import_module(module_name)
    version = getattr(module, "__version__", "unknown")
    path = getattr(module, "__file__", "built-in")
    print(f"  OK  {label:<17} {module_name} version={version} {path}")
PY
}

print_section "Setting up Fingerprint Orchestrator environment"
cd "${ROOT_DIR}"
create_project_env
install_python_deps
run_sanity_checks

print_section "Setup complete"
printf "Activate with: source %s/bin/activate\n" "${VENV_DIR}"
printf "Run API:       fingerprint-orchestrator-api\n"
printf "Run CLI:       fingerprint-orchestrator-cli\n"
printf "Refresh env:   RECREATE_VENV=0 ./scripts/setup_orchestrator_env.sh\n"
