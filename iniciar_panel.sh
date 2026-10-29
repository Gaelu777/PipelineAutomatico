#!/usr/bin/env bash
# Panel de control del pipeline ETL
cd "$(dirname "$0")"
python3 app/servidor.py || {
  echo "No se pudo iniciar. Abre dashboard_offline.html en el navegador:"
  echo "  no necesita Python ni internet."
}
