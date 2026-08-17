FROM python:3.12-slim

WORKDIR /app
COPY service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY service/app ./app

ENV VM_DATA=/data
VOLUME /data
EXPOSE 8090

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8090"]
