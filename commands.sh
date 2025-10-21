docker run \
    --name my_postgres \
    -p 5432:5432 \
    -e POSTGRES_USER=admin \
    -e POSTGRES_PASSWORD=admin \
    -e POSTGRES_DB=kaspi_lab_db \
    -v postgres_data:/var/lib/postgresql/data \
    postgres:16