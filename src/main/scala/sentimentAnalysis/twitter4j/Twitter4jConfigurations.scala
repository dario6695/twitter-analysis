package sentimentAnalysis.twitter4j

import twitter4j.conf.ConfigurationBuilder

object Twitter4jConfigurations {

  lazy val configBuilder: ConfigurationBuilder = getConfigurations


  private def getConfigurations =  {
    val  env = System.getenv()
    val confBuilder = new ConfigurationBuilder
    confBuilder.setDebugEnabled(true)
      .setOAuthConsumerKey(env.get("CONSUMER_KEY"))
      .setOAuthConsumerSecret(env.get("CONSUMER_SECRET"))
      .setOAuthAccessToken(env.get("ACCESS_TOKEN"))
      .setOAuthAccessTokenSecret(env.get("ACCESS_TOKEN_SECRET"))
      .setJSONStoreEnabled(true)
  }

}
