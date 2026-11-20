FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.34.0 websockets==14.1 paramiko==3.5.0 pydantic==2.10.4

COPY app.py /app/app.py
COPY static /app/static

EXPOSE 8088
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8088"]
