package util

import org.joda.time.DateTime

/**
  * Created by hungdv on 12/04/2017.
  */
object PathController extends Serializable{
  def convertDateToFolderPath(rootFolder: String, date: DateTime): String =  {
    val dateTime = date.toString("MM-dd-yyyy")
    String.format("%s%s/",rootFolder,dateTime)
  }

  def convertDateToFilePath(rootFolder: String, date: DateTime,surfix: String): String =  {
    val dateTime = date.toString("yyyy-MM-dd")
    String.format("%s/%s_%s",rootFolder,dateTime,surfix)
  }

  def convertDateToFilePath(rootFolder: String, date: DateTime,prefix: String,surfix: String): String =  {
    val dateTime = date.toString("yyyy-MM-dd")
    String.format("%s/%s%s_%s",rootFolder,prefix,dateTime,surfix)
  }

  def main(args: Array[String]): Unit = {
    print(convertDateToFolderPath("/home/hungdv/",DateTime.now()))
  }

}

