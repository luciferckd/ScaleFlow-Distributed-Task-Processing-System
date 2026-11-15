FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py database.py redis_client.py ./

CMD ["python", "-u", "main.py"]

