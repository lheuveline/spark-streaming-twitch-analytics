Spark Streaming Twitch Analytics

* Includes :
	- Spark Streaming Custom Receiver for Twitch IRC
	- IRCBot converted from Java
	- Stream Chat Wordcount
	- SparkNLP for streaming sentiment analysis
	- Output to Redis for further analysis or viz ([WIP] Dash repo to display live results)
	
* Usage :
	* Build :
            - ``sbt run [channelName] [batchDuration] [language] [twitch_nickname] [twitch_oauth_token]``
        * Submitting application in local mode :
            - ```spark-submit --master "local[*]" --deploy-mode client target/scala-2.12/Twitch-assembly-0.1.jar [TWITCH_CHANNEL] [BATCH_INTERVAL] [LANGUAGE] [TWITCH_USERNAME] [TWITCH_OAUTH]```
