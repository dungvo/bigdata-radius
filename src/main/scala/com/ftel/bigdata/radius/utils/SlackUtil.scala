package com.ftel.bigdata.radius.utils

import scalaj.http.Http
import com.ftel.bigdata.utils.DateTimeUtil

object SlackUtil {
  private val url = "https://hooks.slack.com/services/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"
  def alert(msg: String) {
    Http(url)
      //.postData("payload={\"username\": \"webhookbot\", \"text\": \"This is posted to #general and comes from a bot named webhookbot.\", \"icon_emoji\": \":ghost:\"}")
      .postData("payload={\"username\": \"webhookbot\", \"text\": \"" + msg + "\"}")
      .proxy("210.245.31.16", 80)
      .asString
  }
  
  val SAMPLE_FROM_SLACK_DOC = """
      {"attachments": [
        {
            "fallback": "Required plain-text summary of the attachment",
            "color": "@@@COLOR@@@",
            "pretext": "Optional text that appears above the attachment block",
            "author_name": "Bobby Tables",
            "author_link": "http://flickr.com/bobby/",
            "author_icon": "http://flickr.com/icons/bobby.jpg",
            "title": "Slack API Documentation",
            "title_link": "https://api.slack.com/",
            "text": "Optional text that appears within the attachment",
            "fields": [
                {
                    "title": "Priority",
                    "value": "High",
                    "short": false
                }
            ],
            "image_url": "http://my-website.com/path/to/image.jpg",
            "thumb_url": "http://example.com/path/to/thumb.png",
            "footer": "Slack API",
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png",
            "ts": 123456789
        }
    ]
}"""

  /**
   * Using ("link_names": 1) for mention 1 user in channel
   */
  val JSON_TEMPLATE = 
"""
{
    "channel": "@Channel",
    "username": "monkey-bot",
    "icon_emoji": ":monkey_face:",
    "link_names": 1,
    "attachments": [
        {
            "fallback": "@fallback",
            "color": "@Color",
            "pretext": "@Pretext",
            "text": "@Text",
            "fields": [
                {
                    "title": "Priority",
                    "value": "High",
                    "short": false
                }
            ],
            "ts": @Time
        }
    ]
}
""" 

  
  //"channel": "#general", "link_names": 1, "username": "monkey-bot", "icon_emoji": ":monkey_face:"
  
//  private def jsonBuilder() {
//    val json = new Json
//  }
  
  def alert(username: String, pretext: String, msg: String, status: Int) {
    val timestamp = DateTimeUtil.now.getMillis / 1000
    val json = JSON_TEMPLATE
      .replace("@fallback", "DNS ALERT PARSE LOG")
      .replace("@Color", if (status == 0) "good" else "danger")
      .replace("@Pretext", (if (status != 0) s"@${username} " else "") + pretext)
      .replace("@Text", msg)
      .replace("@Channel", "#tracking-dns")
      .replace("@Time", timestamp.toString)
    //println(json)
    Http(url)
      .header("Content-type", "application/json")
      //.postData("payload={\"username\": \"webhookbot\", \"text\": \"This is posted to #general and comes from a bot named webhookbot.\", \"icon_emoji\": \":ghost:\"}")
      //.postData("{\"text\":\"This is a line of text.\nAnd this is another one.\"}")
      .postData(json)
      .proxy("210.245.31.16", 80)
      .asString
  }
  
  def main(args: Array[String]) {
    alert("thanhtm", "test", "test", 1)
  }
}