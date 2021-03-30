Spark Streaming Twitch Analytics

* Includes :
	- Spark Streaming Custom Receiver for Twitch IRC
	- IRCBot converted from Java
	- Stream Chat Wordcount
	- SparkNLP for streaming sentiment analysis
	- Output to Redis for further analysis or viz ([WIP] Dash repo to display live results)
	
* Usage :
    - ``sbt run [channelName] [batchDuration] [language] [twitch_nickname] [twitch_oauth_token]``
