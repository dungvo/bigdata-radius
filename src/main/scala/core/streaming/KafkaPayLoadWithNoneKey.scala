package core.streaming
import core.Payload
/**
  * Created by hungdv on 15/03/2017.
  */
case class KafkaPayLoadWithNoneKey[K,V](key: Option[K], value: V) extends Payload[K,V] {
}