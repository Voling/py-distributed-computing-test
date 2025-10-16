FROM python:3.13.8-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "bridge.py"]