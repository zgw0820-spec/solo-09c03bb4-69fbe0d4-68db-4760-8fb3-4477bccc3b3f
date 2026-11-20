FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000/tcp 5004/udp 5006/udp 6000/udp

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
