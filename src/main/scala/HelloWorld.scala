import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark
import org.apache.spark.api.java.function.Function2

import java.io.PrintStream
import java.net.{InetAddress, Socket}
import java.time.LocalTime
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession, functions}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext, Time}

import java.sql.Timestamp
import java.util
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
import scala.math.Ordering.Implicits.infixOrderingOps


object HelloWorld {


  case class BidEvent(eventTime: Long, aucId: Int, perId: Int, bidId: Int, price: Double, processingTime: Timestamp)
  object BidEvent {
    def apply(rawStr: String): BidEvent = {
      val parts = rawStr.split(",")
      BidEvent(java.lang.Long.parseLong(parts(0)), (Integer.parseInt(parts(1))),(Integer.parseInt(parts(2))),Integer.parseInt(parts(3)) ,(java.lang.Double.parseDouble(parts(4))) , new Timestamp(System.currentTimeMillis()) )
    }
  }


  def main(args: Array[String]): Unit = {

    // Create the context with a 1 second batch size
    /*
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val data = ssc.textFileStream("/home/nabil/eclipse-workspace/learnscalaspark/ratings100k.csv")*/
    // Create an input stream with the custom receiver on target ip:port and count the
/*    val sc = new SparkContext("local[*]", "LearnScalaSpark")
    val data = sc.textFile("/home/nabil/eclipse-workspace/learnscalaspark/ratings100k.csv")*/


    val sparkConf = new SparkConf().setAppName("HelloWorld").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setCheckpointDir("/tmp/")
    //172.16.0.254
    val startTime = System.currentTimeMillis()
    val data: DStream[String] = ssc.receiverStream(new CustomReceiver("172.16.0.254", 5000))


    val result = data.map(mapToValues).reduceByKeyAndWindow((a, b) =>
      (a._1, a._2, a._3, a._4, a._5, a._6 + b._6),
      windowDuration = Seconds(2), Seconds(1))

    val result1 = result.reduce((x, y) => if(x._2._6 > y._2._6) x else y)


    val hotBid = result1.map(a=>new Tuple8[Int,Long, Int,Int,Float,Long,Int,Long]
      (a._1, a._2._1, a._2._2, a._2._3, a._2._4, a._2._5, a._2._6, System.currentTimeMillis())
    )

    hotBid.saveAsTextFiles("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv")



    /*  val result = data.map(line => line.split(',')(0).toInt) // Ext
        .countByValueAndWindow(Seconds(2), Seconds(1))

      val hotItem = result.reduce((x, y) => {

          if(x._2 > y._2) {
            println("xxxxxx")
            println(x)
            x
          }
          else {
            print("yyyy")
            println(y)
            (y,System.currentTimeMillis())
          }
      })
  */


    //val value = result.reduceByKey(math.max(_,_))
    //val value = result.reduceByKeyAndWindow(math.max(_,_),Seconds(2), Seconds(1))



   // val k = result.reduce((x, y) => if(x._2 > y._2) x else y)

    //val k = result.maxBy(_._2)


    //println(k)

    //Highbid

/*    val result1 = data.map(mapToValues).reduceByKeyAndWindow((x, y) =>  {
      if(x._4 > y._4) x else y
    },windowDuration = Seconds(2)).repartition(1)

    val k = result1.reduce((x, y) => if(x._2._4 > y._2._4) x else y)

    val highBid = k.map(a=>new Tuple8[Int,Long, Int,Int,Float,Long,Int,Long]
    (a._1, a._2._1, a._2._2, a._2._3, a._2._4, a._2._5, a._2._6, System.currentTimeMillis())
    )

    highBid.saveAsTextFiles("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv")*/


   /* val resulty = data.map(mapToTuple).reduce((x, y) => if(x._2 > y._2) x else y)


    println(resulty)*/


   // result.saveAsTextFiles("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv")
    //k.foreachRDD(t=> t.saveAsTextFile("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv"))
    //val ll = sc.parallelize(Seq(count))



    //val lines = ssc.receiverStream(new CustomReceiver("localhost", 31000))



    //
    //
    // val wordCounts = lines.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Milliseconds(2000), Milliseconds(1000))
    //val result = lines.map(line => line.split(',')(4).toFloat *1.24) // Extract rating from line as float

    /*var conv = data.map(line => line.concat("," +System.currentTimeMillis() ))
     conv = conv.map(line => line.concat("," + line.split(',')(4).toFloat * 1.24 ))

    conv = conv.map(line => line.concat("," + System.currentTimeMillis()))

   // val data = data.map(line => line.split(',')(1).toInt).countByValue(1) // Extract rating from line as float

    conv.saveAsTextFiles("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv")*/






    //data.print()

  //  data.saveAsTextFiles("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv")

  ssc.start()

    ssc.awaitTermination()
    println("Throughput: " + (System.currentTimeMillis() - startTime))

  }






  def addInt( a:Int, b:Int ) : (Int) = {
    var sum:Int = 0
    sum = a + b

    return sum
  }

  def addTime( a:Int, b:Long ) :(Int, (Long,Long)) = {


    return (a, (b,System.currentTimeMillis()))
  }



  def mapToValues(line: String): (Int, (Long, Int,Int,Float,Long,Int)) = {

    val fields = line.split(',')

    return (fields(0).toInt, (fields(1).toLong,fields(2).toInt,fields(3).toInt,fields(4).toFloat,System.currentTimeMillis(), 1))
  }


  def mapToTuple(line: String): (Int, (Int)) = {
    val fields = line.split(',')
    return (fields(0).toInt, (fields(4).toInt))
  }

}
