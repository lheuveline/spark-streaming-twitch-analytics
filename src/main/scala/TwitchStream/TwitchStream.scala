package TwitchStream

object TwitchStream extends App {

  /*

  Stream Twitch Channel Chat to Spark DStream.

  sbt run [channelName] [batchDuration] [twitch_nickname] [twitch_oauth]

  ChannelName : #<ChannelName>
  BatchDuration: in seconds

  Pass channel name with # prefix : #<ChannelName> when running with sbt
  Ex: sbt run #alphacast

  TO-DO :
    - Pass stream name to SparkRunner constructor and create specific table in Redis
    - Sentiment : Output result as CSV timeserie for further analysis / display

  Possible architecture :
    - Dockerize scala app
    - Run dataviz in python from 3rd container

   */

  val channel = args(0)
  val batchDuration = args(1).toLong
  val language = args(2)
  val twitch_nick = args(3)
  val twitch_oauth = args(4)

  val runner = new SparkRunner(channel, batchDuration, language, twitch_nick, twitch_oauth)

  runner.start()

}
