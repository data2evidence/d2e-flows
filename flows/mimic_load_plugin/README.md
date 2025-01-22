### Mount mimiciv directory to Prefect Docker container
Add mimiciv directory which contains hosp and icu sub-directories `path/to/mimic:/app/mimic` to the `PREFECT_DOCKER_VOLUMES` environment variable list for trex service in docker-compose.yml

### Create Schemas
The corresponding tables are created and populated with csv files in mimiciv directory
1. mimiciv_hosp
2. mimiciv_icu
3. mimiciv_derived

