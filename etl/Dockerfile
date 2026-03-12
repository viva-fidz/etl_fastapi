FROM python:3.9-slim-buster

WORKDIR /app

COPY etl/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ .

CMD ["python", "main.py"]