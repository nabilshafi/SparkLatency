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
import org.apache.spark.sql.functions.{avg, col, current_timestamp, explode, from_json, max, split, to_csv, window}
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
    /*val sc = new SparkContext("local[*]", "LearnScalaSpark")
    val data = sc.textFile("/home/nabil/eclipse-workspace/learnscalaspark/ratings100k.csv")*/


    val sparkConf = new SparkConf().setAppName("HelloWorld").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setCheckpointDir("/tmp/")

    val data = ssc.receiverStream(new CustomReceiver("localhost", 31000))

    val result = data.map(line => line.split(',')(1).toInt) // Ext
      .countByValueAndWindow(Seconds(2), Seconds(1))

    val value = result.reduceByWindow((x, y) => if(x._2 > y._2) x else y,Seconds(2), Seconds(1))

    //val value = result.reduceByKey(math.max(_,_))
    //val value = result.reduceByKeyAndWindow(math.max(_,_),Seconds(2), Seconds(1))



   // val k = result.reduce((x, y) => if(x._2 > y._2) x else y)

    //val k = result.maxBy(_._2)

    value.print()
    //println(k)

   /* val resulty = data.map(mapToTuple).reduce((x, y) => if(x._2 > y._2) x else y)


    println(resulty)*/


    value.saveAsTextFiles("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv")
    //k.foreachRDD(t=> t.saveAsTextFile("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv"))


    //val ll = sc.parallelize(Seq(count))



    //val lines = ssc.receiverStream(new CustomReceiver("localhost", 31000))



    //
    //
    // val wordCounts = lines.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Milliseconds(2000), Milliseconds(1000))
    //val result = lines.map(line => line.split(',')(4).toFloat *1.24) // Extract rating from line as float

/*    var data = lines.map(line => line.concat("," + line.split(',')(4).toFloat * 1.24 + "," + System.currentTimeMillis() ))

    data = data.map(line => line.concat("," + System.currentTimeMillis()))*/

   // val data = data.map(line => line.split(',')(1).toInt).countByValue(1) // Extract rating from line as float

    //data.saveAsTextFiles("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv")






    //data.print()

  //  data.saveAsTextFiles("/home/nabil/eclipse-workspace/learnscalaspark/data/filess.csv")

  ssc.start()
    ssc.awaitTermination()


  }



 /*   val startTime = System.nanoTime

    val sc = new SparkContext("local[*]", "LearnScalaSpark")
    // Read a text file
    var data = sc.textFile("/home/nabil/eclipse-workspace/learnscalaspark/ratings1m.csv")
    // Extract the first row which is the header
    //val header = data.first()


  data = data.map(line => line.concat("," + System.nanoTime()))


    data = data.map(line => line.concat("," + System.nanoTime()))

    //result.foreach(println)


    data.coalesce(1).saveAsTextFile("/home/nabil/eclipse-workspace/learnscalaspark/ratings1m")
    println("Throughput: " + (System.nanoTime - startTime))}
*/







/*    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()*/

/*    val startTime = System.nanoTime



    val sparkConf = new SparkConf().setAppName("simpleReading").
      setMaster("local[2]")
    //set spark configuration
    val sparkContext = new SparkContext(sparkConf)
    // make spark context
    val sqlContext = new SQLContext(sparkContext) // make sql context

    val df = sqlContext.read.
      format("com.databricks.spark.csv").
      option("header", "true").
      option("inferSchema", "true").load("/home/nabil/eclipse-workspace/learnscalaspark/ratings.csv")

    //load data from a file

    val selectedCity = df.select("movieId")

selectedCity.show()
    val selectMake = df.select("userId", "rating") //select particular column
    selectMake.show()
    //show make column

    val tempTable = df.registerTempTable("my_table")
    //makes a temporary table
    val usingSQL = sqlContext
      .sql("select rating from my_table")
    //show all the csv file's data in temp table
    usingSQL.show()
    val stopTime = System.nanoTime
    val duration = (stopTime - startTime) / 1e9d
    System.out.println(duration)
    System.out.println(100000/duration)
*/

    /* var data = spark.read.csv("/home/nabil/eclipse-workspace/learnscalaspark/ratings.csv")
   // val df = spark.read.json("/home/nabil/eclipse-workspace/learnscalaspark/people.json")
   val header = data.select("movieId")

    print(header)
*/



  def addInt( a:Int, b:Int ) : (Int) = {
    var sum:Int = 0
    sum = a + b

    return sum
  }

  def mapToTuple(line: String): (Int, (Int)) = {
    val fields = line.split(',')
    return (fields(0).toInt, (fields(4).toInt))
  }

}
