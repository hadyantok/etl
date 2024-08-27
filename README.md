# etl

Repo will only be accessible until 30 Sept 2024

1. Init compute VM and Enable required port in firewall

2. Install Docker `https://docs.docker.com/engine/install/ubuntu/`

3. Install docker compose

```
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose

docker-compose --version
```

4. clone repo and set dag path in docker-compose.yaml

```
git clone git@github.com:hadyantok/etl.git
```

5. Install airflow

```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.0/docker-compose.yaml'

change dags path to repo dags

echo -e "AIRFLOW_UID=$(id -u)" > .env

sudo docker compose up airflow-init

sudo docker compose up
```

6. Build task docker image

```
cd etl/setup/

docker build . -f Dockerfile.task -t task_runner
```
