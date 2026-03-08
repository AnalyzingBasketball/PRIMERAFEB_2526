name: Actualizar Datos FEB

on:
  schedule:
    # Ejecuta el script todos los días a las 04:00 AM UTC
    - cron: '0 4 * * *'
  workflow_dispatch:
    # Permite ejecutarlo manualmente desde GitHub cuando quieras

jobs:
  actualizar_datos:
    runs-on: ubuntu-latest

    steps:
      - name: Descargar el repositorio
        uses: actions/checkout@v4

      - name: Configurar Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Instalar dependencias
        run: |
          pip install -r requirements.txt

      - name: Ejecutar el script ETL
        run: |
          python main.py

      - name: Guardar los CSV generados en el repositorio
        run: |
          git config --local user.name "GitHub Actions Bot"
          git config --local user.email "actions@github.com"
          git add data/*.csv
          git add data/raw_api/*.json
          git commit -m "Actualización automática de datos FEB" || echo "No hay cambios para subir"
          git push
