package sentimentAnalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.scalatest.flatspec.AnyFlatSpec
import sentimentAnalysis.dataModel.entities.{TwitterDeepInfo, TwitterStatus}
import sentimentAnalysis.dataModel.repositoryDto.TwitterDeepInfoRepoDto
import sentimentAnalysis.spark.SparkJobExecutorService
import sentimentAnalysis.spark.schema.Schemas.{twitterDeepInfoRestDtoSchema, twitterDeepInfoSchema, twitterStatusSchema}


class SparkJobExecutorTest extends AnyFlatSpec {

  private val twitterJobExecutorService = new SparkJobExecutorService()

  private val spark: SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  private val sampleDF = spark.createDataset(Seq(
    """{"id": 1, "text": "not containing keyword", "timestamp_ms": "783286726837"}""",
    """{"id": 2, "text": "this is IKEA uppercase", "timestamp_ms": "783286726838"}""",
    """{"id": 3, "text": "this is ikea lowercase", "timestamp_ms": "783286726839"}"""
  ))
    .select(from_json(col("value"), twitterStatusSchema).as("actualValue"))
    .selectExpr("actualValue.*")
    .as[TwitterStatus]


  private val sampleDeepDF = spark.createDataset(Seq(
    """{"id": 1, "text": "not containing keyword", "timestamp_ms": "783286726837","user": {"id": 1,"location": "italy"}, "entities": {"hashtags":["{test1}", "{sustainability}"] } }""",
    """{"id": 2, "text": "this is IKEA uppercase", "timestamp_ms": "783286726838","user": {"id": 2,"location": "italy"}, "entities": {"hashtags":["{test2}"] } }""",
    """{"id": 3, "text": "this is ikea lowercase", "timestamp_ms": "783286726839","user": {"id": 3,"location": "france"}, "entities": {"hashtags":["{test3}", "{sustainability}"] } }"""
  ))
    .select(from_json(col("value"), twitterDeepInfoSchema).as("actualValue"))
    .selectExpr("actualValue.*")
    .as[TwitterDeepInfo]


  "filterIkeaTwitterStatus" should "return all the tweets that had the ikea word in the text" in {

    val expectedValue = spark.createDataset(Seq(
      """{"id": 2, "text": "this is IKEA uppercase", "timestamp_ms": "783286726838"}""",
      """{"id": 3, "text": "this is ikea lowercase", "timestamp_ms": "783286726839"}"""
    ))
      .select(from_json(col("value"), twitterStatusSchema).as("actualValue"))
      .selectExpr("actualValue.*")
      .as[TwitterStatus]

    val result = twitterJobExecutorService.filterIkeaTwitterStatus(sampleDF)

    assert(expectedValue.except(result).isEmpty)
  }




  "operationsOverDeepTweets" should "return all the tweets that had the ikea word in the text and sustainability as hastag" in {

    val expectedValue = spark.createDataset(Seq(
      """{"id": 3, "text": "this is ikea lowercase", "timestamp_ms": "783286726839","userId": 3,"userLocation": "france", "hashtags":"{test3}{sustainability}"  }"""
    ))
      .select(from_json(col("value"), twitterDeepInfoRestDtoSchema).as("actualValue"))
      .selectExpr("actualValue.*")
      .as[TwitterDeepInfoRepoDto]

    val result = twitterJobExecutorService.operationsOverDeepTweets(sampleDeepDF)

    assert(expectedValue.except(result).isEmpty)
  }






}