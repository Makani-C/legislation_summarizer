# Legislation Summarizer
Repository for all POC code for the KnowYourBill project

## Pipeline 
####  `/pipeline`
Pulls data from a Legiscan database (currently MariaDB on the same EC2 server), summarizes using LLMs and writes to RDS

## Application
#### `/app`
Serves up RDS data in a FastAPI application layer

## Databases
#### `/database`
Contains shared database connection code

## Testing/Deployment
Docker images exist for the `application` and `pipeline` components.
These are created and published using the github actions workflows
