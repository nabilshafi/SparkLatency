import java.time.LocalTime

import org.apache.spark.sql.functions.{current_timestamp, to_csv}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}


object HelloWorld {

  def main(args: Array[String]): Unit = {

    val startTime = System.nanoTime

    val sc = new SparkContext("local[*]", "LearnScalaSpark")
    // Read a text file
    var data = sc.textFile("/home/nabil/eclipse-workspace/learnscalaspark/ratings1m.csv")
    // Extract the first row which is the header
    //val header = data.first()


  data = data.map(line => line.concat("," + System.nanoTime()))


    data = data.map(line => line.concat("," + System.nanoTime()))

    //result.foreach(println)


    data.coalesce(1).saveAsTextFile("/home/nabil/eclipse-workspace/learnscalaspark/ratings1m")
    println("Throughput: " + (System.nanoTime - startTime))

  }






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







}
