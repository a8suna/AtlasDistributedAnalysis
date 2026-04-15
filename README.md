# **Distributed ATLAS Data Analysis**

This project implements a distributed data processing pipeline for analysing particle physics data from the ATLAS Collaboration experiment at the CERN Large Hadron Collider.

The system runs the H → ZZ → 4ℓ analysis using publicly available ATLAS Open Data. ROOT files are processed in parallel by multiple worker containers, and the results are merged to produce the four-lepton invariant mass distribution showing the Higgs boson signal near 125 GeV.

Jobs are distributed through a message queue using RabbitMQ. Each worker processes one file independently, making the workload embarrassingly parallel and easy to scale across many containers using Docker and Docker Swarm.

## Architecture
The system uses the following components:

**- Controller** - creates jobs and collects results

**- Workers** - recive jobs and run the analysis code

**- Analysis** - processes ROOT files and computes NumPy arrays

**- Message broker** - distributes jobs to workers

### Technologies Used
- RabbitMQ - message queue for distributing jobs
- Docker - containerisation of controller and workers
- Docker Swarm - orchestration and scaling of workers across machines

## Running the system
The system can be run either locally using Docker Compose or in distributed mode using Docker Swarm.
### Local Deployment - Docker Compose
#### Start the containers

`docker compose up --build`

This command builds the Docker images and starts all required containers, including the controller, workers, and the RabbitMQ message broker.

#### Changing the number of workers
The number of worker containers is controlled by the `replicas` field in the `docker-compose.yml` file.

To run the system with a different number of workers:

1. Stop the running containers:

`docker compose down`

2. Edit the `replicas` value in `docker-compose.yml`

3. Restart the system:

`docker compose up --build`

This will rebuild and launch the containers with the updated number of worker replicas.

### Distributed Deployment - Docker Swarm

To run the system across multiple machines, Docker Swarm can be used.

#### Step 1 - Intialise the Swarm

`docker swarm init`

#### Step 2 - Build the images

`docker build -t atlas-distributed-analysis-controller:latest .`

`docker build -t atlas-distributed-analysis-worker:latest .`

#### Step 3 - Deploy the stack:

`docker stack deploy -c docker-stack.yml atlas`

#### Check the service status

`docker stack services atlas`

#### Scale the number of workers

`docker service scale atlas_worker=10`

#### View logs
Worker logs:
`docker service logs -f atlas_worker`

Controller log:
`docker service logs -f atlas_controller`

#### Stop the system

`docker stack rm atlas`

`docker swarm leave --force`
