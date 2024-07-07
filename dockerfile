FROM python:3.12.4-slim

RUN pip install --upgrade pip

COPY ./src /anacostia

RUN pip install -e /anacostia --no-cache-dir

WORKDIR /tests

CMD ["python", "main.py"]