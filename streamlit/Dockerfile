# Usa un'immagine base con Python
FROM python:3.10

# Imposta la directory di lavoro all'interno del container
WORKDIR /app

# Copia i file locali nella cartella del container
COPY . .

# Installa le dipendenze
RUN pip install --no-cache-dir -r requirements.txt

# Espone la porta usata da Streamlit
EXPOSE 8501

# Comando per avviare Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.enableCORS=false"]
