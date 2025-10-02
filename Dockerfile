FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y build-essential libz-dev libjpeg62-turbo-dev
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-m", "src.bot"]
