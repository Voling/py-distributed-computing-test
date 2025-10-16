FROM python:3.13.8-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["celery", "-A", "tasks", "worker", "-l", "info", "-P", "solo"]