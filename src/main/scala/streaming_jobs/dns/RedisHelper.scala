package streaming_jobs.dns
import scala.tools.nsc.interpreter.Completion.Candidates
import scala.util.control.Breaks._

import scala.collection.mutable.ListBuffer
/**
  * Created by hungdv on 31/10/2017.
  */
object RedisHelper {
  private val NOT_PRESENTED_IN_CACHE = "-1"
  private val RECORD_OUT_DATE = "0"
  def main(args: Array[String]): Unit = {
    val list = List("A,1","B,20","C,11","D,9","E,12")
    println(mapping(list,13))
  }
  def mapping(candidates: scala.List[String],timeOfTarget: Long): String = {
    if(candidates == null || candidates.isEmpty){
      NOT_PRESENTED_IN_CACHE
    }else{
      //println("Candidates : " + candidates + " " + timeOfTarget)
      val tupple: Seq[(String, Long)] = candidates.map{ x =>
        val arr: Array[String] = x.split(",")
        if(arr.length == 2){
          (arr(0),arr(1).toLong)
        }
        else{
          (null,0L)
        }
      }.toList.filter(x => x._1 != null ).sortWith(_._2 > _._2)
      var target = RECORD_OUT_DATE
      breakable{
        for(i <- 0 to tupple.length - 1){
          if(tupple(i)._2 < timeOfTarget){
            target = tupple(i)._1
            break
          }
        }
      }
      target
    }
  }

}
