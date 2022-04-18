package sentimentAnalysis.dataModel.entities

case class TwitterDeepInfo(
                            id: Long,
                            text: String,
                            timestamp_ms: String,
                            user: TwitterUser,
                            entities: TwitterEntities
                          )


case class TwitterUser(
                        id: Long,
                        location: String
                      )

case class TwitterEntities(
                            hashtags: List[String]
                          )