FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir \
    -r requirements.txt \
    streamlit \
    pandas

COPY b2_dedup.py b2_gui.py file_utils.py ./

# Data dir (DB, cache, GUI config) — bind-mount ./data here at runtime
RUN mkdir -p /app/data

EXPOSE 8501

# Default: run the web UI
CMD ["python", "b2_dedup.py", "serve", "--port", "8501"]
