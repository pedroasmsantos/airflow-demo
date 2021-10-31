# Airflow Demo

This repository is to experiment with Apache Airflow.

## Usage
1. There's a docker compose file to instantiate all the dependencies, such as the Apache Airflow components (Web Server, Scheduler, Worker), Postgres, Redis and Kafka.
    ```
    docker-compose up
    ```

1. Access http://localhost:8080/
 
1. Login
    * user:airflow
    * pass:airflow
 
1. Setup connections for the DAGs (admin -> connections -> add connection)
  * Postgres Connection
    * conn id: postgres
    * conn type: Postgres
    * host: postgres
    * login: airflow
  * Users API Connection
    * conn_id: user_api
    * conn type: Http
    * host: https://randomuser.me/
