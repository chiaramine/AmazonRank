import java.text.SimpleDateFormat
import java.util.Date

import Util.Utils.disableWarning
import org.apache.spark.sql.SparkSession

object Main {

   def main(args: Array[String]): Unit = {
    disableWarning()



     val dateFormatter = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss")
     var submittedDateConvert = new Date()
     var s3Bucket = "aws-logs-743342462495-us-east-1"
     var pathJson : String = s"s3n://$s3Bucket/"+ dateFormatter.format(submittedDateConvert) + "/"



    val t0 = System.nanoTime()


    val DEMO = false
    val LAMBDA = 20
    val ITER = 10
    val NUM_PARTITIONS = 200
    val path = "s3://aws-logs-743342462495-us-east-1/videogames_25k_utenti.csv" //minidataset composto da una dozzina di utenti


    var timeout: Int = 3000


    val spark = SparkSession
      .builder
      .appName("AmazonRank")
      .getOrCreate()
    val sc = spark.sparkContext



        println("INIZIO ELABORAZIONE")
        Rank.setPath(pathJson,timeout)
        Rank.start(path, sc, LAMBDA,DEMO,ITER,NUM_PARTITIONS)

    println("\nFINE ELABORAZIONE")
    println(s"\nRiepilogo: \n-> file: ${path} \n-> demo: ${DEMO} \n-> ITER: ${ITER} \n-> LAMBDA: ${LAMBDA} \n-> TIMEOUT: ${timeout}) \n-> Tempo totale: " + (System.nanoTime()-t0)/1000000000f + "s")


    sc.stop()
  }


}
