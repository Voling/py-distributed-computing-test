FROM python:3.13.8-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["celery", "-A", "tasks", "worker", "-l", "info", "-P", "solo"]