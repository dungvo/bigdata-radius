package streaming_jobs.conn_jobs

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

/** Singleton object to get Accumulator.
  * Created by hungdv on 08/05/2017.
  */
//FIXME : Change to Generic type of AccumulatorV2 not fixed ConcurrenHashMapAcc
object SingletonAccumulator {
  // See volatile variable !.
  @volatile private var instance: ConcurrentHashMapAccumulator = null
  def getInstance(sc: SparkContext): ConcurrentHashMapAccumulator = {
    if(instance == null){
      synchronized{
        if(instance == null){
          instance = new ConcurrentHashMapAccumulator()
          sc.register(instance)
        }
      }
    }
    instance
  }

}
