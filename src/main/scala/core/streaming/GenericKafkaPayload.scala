package core.streaming
import core.Payload
/**
  * Created by hungdv on 15/03/2017.
  */

case class GenericKafkaPayload[K,V](key: K, value: V) extends Payload[K,V]{
}
