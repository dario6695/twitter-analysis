package sentimentAnalysis.dataModel

import sentimentAnalysis.dataModel.entities.TwitterDeepInfo
import sentimentAnalysis.dataModel.repositoryDto.TwitterDeepInfoRepoDto

object TwitterDeepInfoMapper{

  def toDto(entity: TwitterDeepInfo): TwitterDeepInfoRepoDto = {
    TwitterDeepInfoRepoDto(
      id = entity.id,
      text = entity.text,
      timestamp_ms = entity.timestamp_ms,
      userId = entity.user.id,
      userLocation = entity.user.location,
      hashtags = entity.entities.hashtags.foldRight("")(_ + _)
    )

  }

}
