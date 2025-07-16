FROM python:3.10-slim

# Install build tools and libpq-dev (PostgreSQL headers)
RUN apt-get update && \
    apt-get install -y build-essential libpq-dev gcc && \
    apt-get clean

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
