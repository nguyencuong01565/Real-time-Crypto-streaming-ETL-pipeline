FROM python:3.11-slim

WORKDIR /app

COPY Requiment.txt .

RUN pip install --no-cache-dir -r Requiment.txt

COPY Producer.py .

CMD ["python", "Producer.py"]





