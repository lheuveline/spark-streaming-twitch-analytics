package TwitchStream

import java.io._
import java.net._
import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class IRCBot(val channel: String, val nick: String, val pass:String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  // The server to connect to and our details.
  val server: String = "irc.chat.twitch.tv"
  var line: String = null

  @throws[Exception]
  def read(): Unit = {

    // Connect directly to the IRC server.
    val socket:Socket = new Socket(server, 6667)
    val writer:BufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
    val reader:BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream))

    // Log on to the server.
    System.out.println(s"Trying to authenticate as $nick")
    writer.write("PASS " + pass + "\r\n")
    writer.write("NICK " + nick + "\r\n")
    writer.flush()
    System.out.println(s"Authentication request sent.")

    try {
      Stream.continually(reader.readLine()).takeWhile(_ != ":tmi.twitch.tv 376 " + nick + " :>") foreach { line =>
        println(line.mkString)
      }
    } catch {
      case e: java.net.SocketException => read() // If SocketException, retry to connect
    }

    // Join the channel.
    println("Trying to join " + channel)
    writer.write("JOIN " + channel + "\r\n")
    writer.flush()

    Stream.continually(reader.readLine()).takeWhile(_ != null) foreach { line =>
      if (line.contains("PING ")) {
        writer.write("PONG " + line.substring(5) + "\r\n")
        writer.flush()
      }
      else if (line.contains("PRIVMSG")) {
        val msg_ts = new Timestamp(System.currentTimeMillis()).getTime
        store(msg_ts + line)
      }
    }
  }

  def onStart(): Unit = {
    val botThread = new Thread {
      override def run() { read() }
    }
    botThread.start()
  }

  def onStop(): Unit = {
    // Thread should stop by itself
  }
}
