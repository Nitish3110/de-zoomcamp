# Docker, SQL, Data Ingestion

-> docker compose up -d
-> docker build -t taxi_ingestion:0.0.2 .

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

docker run -it \
    --network=1-docker-postgres_default \
    taxi_ingest:0.0.2 \
        --user=root \
        --password=root \
        --host=pgdatabase \
        --port=5432 \
        --db=nyc_taxi \
        --table_name=yellow_taxi_trips \
        --url=${URL}

URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

docker run -it \
    --network=1-docker-postgres_default \
    taxi_ingest:0.0.2 \
        --user=root \
        --password=root \
        --host=pgdatabase \
        --port=5432 \
        --db=nyc_taxi \
        --table_name=taxi_zones \
        --url=${URL}