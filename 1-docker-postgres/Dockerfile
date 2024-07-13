FROM python

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /jobs

COPY ingest_data.py ingest_data

# start the script here , script should contain whole ETL pipeline 
# functionality
ENTRYPOINT [ "python", "" ]