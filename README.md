#Data Migration
## Prerequisites

- NodeJS (v12)
- Docker, Docker Compose

## Configuration
Configuration for the application is at `config/default.js` The following parameters can be set in config files

- PORT: the server port
- topic: kafka topic NAME&PARTITION
- topic_error: kafka topic NAME&PARTITION, which handle some errors
- RETRY_COUNTER: kafka max retry counter
- SOURCE: SOURCE database information. This will be in `INFORMIX` now
- DESTINATION: DESTINATION database information. This will be in `POSTGRESQL` now
- db: the DBs URI and OPTIONS

##Environment setup
###Informix/Kafka
open the pdf for more Details `Migration from Informix to Postgres.docx.pdf`

###Postgres
run `docker-compose up -d`

###Producer and Consume
- open a terminal
- run `npm i`
- run `npm run producer`
- open new terminal
- run `npm run consumer`

## Verification
- create testdb schema via `http://localhost:8081`
- Connect with Informix server which we are running already
- run cd /home/informix/trunk/dev2
- run dbaccess -e testdb saveAuditOnFile.sql
- This will generate a json file in /tmp. Also it will post the json file to Producer
- We can see the db detail at `http://localhost:8081`
- delete line 54:`DELETE FROM test where name = 'name'; `
- run dbaccess -e testdb saveAuditOnFile.sql again
- see the db detail at `http://localhost:8081`
