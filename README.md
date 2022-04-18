# How to run
* Go in the directory where the project is located
* Execute `docker-compose up` to start kafka and postgres containers
* In your project set the following ENV variables, the value can be obtained registering for the twitter developer api utilization:
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


# Available Jobs

* persistTweetsStreaming: persist basic data model into `public.twitterstatus` table using spark structured streaming interface
  ```
  TwitterStatus(id: Long, text: String, timestamp_ms: String)
  ```
* persistTweetsBatch: persist basic data model into `public.twitterstatusBatch` table using spark batch interface
* persistDeepTweetsStreaming: persist a more insightful model into `public.twitterdeepstatus` using spark structured streaming interface
  ``` 
  case class TwitterDeepInfoRepoDto(
                                   id: Long,
                                   text: String,
                                   timestamp_ms: String,
                                   userId: Long,
                                   userLocation: String,
                                   hashtags: String
                                 )
  ```
  
*the right job to be executed can be selected into the main function of TwitterProject



# Pipeline

![Alt text](images/twitterPipe.drawio.png?raw=true "Title")

* Tweet data are extracted using the twitted4j API that extract that in streaming
* These tweets are sent into Apache Kafka to allow efficient consumption by spark and are eventually made available to more consumers
* Spark Streaming/Batch (depending on the executed job) will use Kafka as a source and Postgres as a sink, making the necessary transformations 
* data are persisted in PostgresSQL and available for queries and analytics





