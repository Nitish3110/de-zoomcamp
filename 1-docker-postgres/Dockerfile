FROM python:latest

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /jobs

COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]