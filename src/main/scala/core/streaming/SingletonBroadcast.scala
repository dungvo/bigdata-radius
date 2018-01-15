package core.streaming

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import parser._
import streaming_jobs.conn_jobs.ConcurrentHashMapAccumulator
import streaming_jobs.dns.DNSParser

import scala.collection.{Map, mutable}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

/**  Singleton  object to get Broadcast variable.
  * Created by hungdv on 08/05/2017.
  */
// FIXME : Don't repeat your self!!!!!!!
object DurationBoadcast {

  @volatile private var instance: Broadcast[FiniteDuration] = null

  def getInstance(sc: SparkContext,duration: FiniteDuration): Broadcast[FiniteDuration] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(duration)
        }
      }
    }
    instance
  }
}
//FIXME : use AbtractLogParser instead  of child class
/*
object ParserBoacast {

  @volatile private var instance: Broadcast[AbtractLogParser] = null

  def getInstance(sc: SparkContext,parser: AbtractLogParser): Broadcast[AbtractLogParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}
*/

object ParserBoacast {

  @volatile private var instance: Broadcast[ConnLogParser] = null

  def getInstance(sc: SparkContext,parser: ConnLogParser): Broadcast[ConnLogParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}

object MapBroadcast {

  @volatile private var instance: Broadcast[Map[String,String]] = null

  def getInstance(sc: SparkContext,map: mutable.Map[String,String]): Broadcast[Map[String,String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(map)
        }
      }
    }
    instance
  }
}

object LoadLogBroadcast {

  @volatile private var instance: Broadcast[LoadLogParser] = null

  def getInstance(sc: SparkContext,parser: LoadLogParser): Broadcast[LoadLogParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}

object DNSParsergBroadcast {

  @volatile private var instance: Broadcast[DNSParser] = null

  def getInstance(sc: SparkContext,parser: DNSParser): Broadcast[DNSParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}


object InfParserBroadcast {

  @volatile private var instance: Broadcast[INFLogParser] = null

  def getInstance(sc: SparkContext,parser: INFLogParser): Broadcast[INFLogParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}
object OpsParserBroadcast {

  @volatile private var instance: Broadcast[OpsviewParser] = null

  def getInstance(sc: SparkContext,parser: OpsviewParser): Broadcast[OpsviewParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}
object NocParserBroadcast {

  @volatile private var instance: Broadcast[NocParser] = null

  def getInstance(sc: SparkContext,parser: NocParser): Broadcast[NocParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}
//FIXME : Using this object for all parser.
object ParserBoadcast {
  @volatile private var instance: Broadcast[AbtractLogParser] = null

  def getInstance(sc: SparkContext,parser: AbtractLogParser): Broadcast[AbtractLogParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}