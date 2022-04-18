package sentimentAnalysis

import org.scalatest.flatspec.AnyFlatSpec
import sentimentAnalysis.dataModel.TwitterDeepInfoMapper
import sentimentAnalysis.dataModel.entities.{TwitterDeepInfo, TwitterEntities, TwitterUser}
import sentimentAnalysis.dataModel.repositoryDto.TwitterDeepInfoRepoDto

class TwitterDeepInfoMapperTest extends AnyFlatSpec {

  private val twitterDeepInfoMapper = new TwitterDeepInfoMapper()

  private val initialValue = TwitterDeepInfo(
    id = 1,
    text = "test",
    timestamp_ms = "21212121",
    user = TwitterUser(1, "italy"),
    entities = TwitterEntities(List("{ok}", "{ko}"))
  )

  "The TwitterDeepInfoMapper" should "convert the entity TweetDeepIfo in the dto object to be persisted" in {

    val expectedResult = TwitterDeepInfoRepoDto(
      id = 1,
      text = "test",
      timestamp_ms = "21212121",
      userId = 1,
      userLocation = "italy",
      hashtags = "{ok}{ko}"
    )

    val result = twitterDeepInfoMapper.toDto(initialValue)

    assertResult(expectedResult)(result)
  }

}
