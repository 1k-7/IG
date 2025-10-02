FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y zlib1g-dev
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-m", "src.bot"]
