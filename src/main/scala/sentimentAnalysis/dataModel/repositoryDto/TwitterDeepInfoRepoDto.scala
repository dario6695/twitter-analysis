package sentimentAnalysis.dataModel.repositoryDto

case class TwitterDeepInfoRepoDto(
                                   id: Long,
                                   text: String,
                                   timestamp_ms: String,
                                   userId: Long,
                                   userLocation: String,
                                   hashtags: String
                                 )
