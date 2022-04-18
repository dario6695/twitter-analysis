# How to run
* Go in the directory where the project is locate
* Execute `docker-compose up` to start kafka and posgres containers
* In your project set the followinf ENV variables, the value can be obtained registering for the twitter developer api utilization:
  * CONSUMER_KEY
  * CONSUMER_SECRET
  * ACCESS_TOKEN
  * ACCESS_TOKEN_SECRET
* Run the following class sentimentAnalysis.TwitterProject
* Persisted data can be accessed and queried inside the container:
  * execute `docker exec -it postgresContainer psql -U docker -d twitter`
  * here you can issue SQL queries to the DB created by the spark job sink connector, es: `select * from public.twitterstatus;`
    * some useful commands to see the state of postgres:
      * \l: list all databases
      * \c <db name>: connect to a certain database
      * \dt: list all tables in the current database

# Pipeline






