# container_example



## Start the database


docker compose up --build -d 


## Start the pipeline

#Load Source Data which is currently housed in container real world it would not be in the container
docker compose run --rm app python main.py


## Run dbt snapshots to identify incrementals/deltas
docker compose run --rm dbt docs generate  
## Verify Source Data
docker compose run  --rm dbt source freshness
## Run dbt snapshots to identify incrementals/deltas
docker compose run --rm dbt snapshot  
## Run dbt models
docker compose run --rm dbt run  
## Run dbt tests
docker compose run --rm dbt test  
