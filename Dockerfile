FROM apache/airflow:3.3.1-python3.12

USER airflow

# Копируем файлы конфигурации для установки зависимостей
COPY --chown=airflow:root pyproject.toml /opt/airflow/pyproject.toml
COPY --chown=airflow:root src /opt/airflow/src

# Устанавливаем пакет из src и зависимости в editable-режиме (-e)
RUN pip install --no-cache-dir -e /opt/airflow
