FROM python:3.10-alpine

WORKDIR /app

COPY requirements.txt requirements.txt

COPY init_frost_db.py init_frost_db.py

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD ["python", "init_frost_db.py"]