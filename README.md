# **Distributed ATLAS Data Analysis**

This project implements a distributed data processing pipeline for analysing particle physics data from the ATLAS Collaboration experiment at the CERN Large Hadron Collider.

The system runs the H → ZZ → 4ℓ analysis using publicly available ATLAS Open Data. ROOT files are processed in parallel by multiple worker containers, and the results are merged to produce the four-lepton invariant mass distribution showing the Higgs boson signal near 125 GeV.

Jobs are distributed through a message queue using RabbitMQ. Each worker processes one file independently, making the workload embarrassingly parallel and easy to scale across many containers using Docker and Docker Swarm.

## Architecture
The system uses the following components:
**- Controller** - creates jobs and collects results
**- Workers** - process ROOT files and compute NumPy arrays
**- Message broker** - distributes jobs to workers

- RabbitMQ - message queue for distributing jobs
- Docker - containerisation
- Docker Swarm - orchestration and scaling

## Running the system
### Local Deployment
To run the containers:
'docker compose up --build'
To specify the number of workers, the replicas in the docker-compose.yml file should be edited.
To retest with a different number of workers, the docker compose needs to close and restart.
'docker compose down'
'docker compose up --build'

### Distributed Deployment (Docker Swarm)
Step 1 - Intialise the Swarm
'docker swarm init'
Step 2 - Build the images
'docker build -t atlas-distributed-analysis-controller:latest .'
'docker build -t atlas-distributed-analysis-worker:latest .'
Step 3 - Deploy the stack:
'docker stack deploy -c docker-compose.yml atlas'
Step 4 - Check the service status
'docker stack services atlas'
Step 5 - Scale workers 
'docker service scale atlas_worker=10'
Step 6 - check logs
'docker service logs atlas_worker'     
'docker service logs atlas_merger'
Step 7 - To tear down the system and leave swarm
'docker stack rm atlas'
'docker swarm leave --force'