package pack

//required libraries for streaming on kinesis
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.{KinesisInputDStream, KinesisUtils}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._ //contains StreamingContext class and Seconds() --- 2
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.dynamodbv2.model.BillingMode
import org.apache.spark.streaming.kinesis.KinesisUtils
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import org.apache.hadoop.fs.s3a.S3AFileSystem


//importing kinesis utils
import org.apache.spark.streaming.kinesis.KinesisUtils

object obj {
  
  //creating a user defined function to convert bytearray format from kinesis to string
  def b2s(a: Array[Byte]): String = new String(a)

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("first")
                              .setMaster("local[*]")
                              .set("spark.driver.allowMultipleContexts", "true")
                              .set("AWS_ACCESS_KEY","ACCESS_KEY")
                              .set("AWS_SECRET_KEY","SECRET_KEY")
                              .set("AWS_CBOR_DISABLE","true") //If this is enabled additional authentication details required for AWS
                              
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate( )
    import spark.implicits._
    
    // initializing the streaming context and set for how many seconds spark hits kafka ---- 3
    val ssc = new StreamingContext(conf, Seconds(1))
    
   
   val stream = KinesisUtils.createStream(
                                          ssc,
                                          "group3",  // unique id for this consumer, MQ keeps track of the last message sent to it. Useful for sending only incremental data
                                          "awsmq", // MQ_NAME to read from
                                          "https://kinesis.us-east-1.amazonaws.com", //aws end point where the mq was created
                                          "us-east-1", //location of mq
                                          InitialPositionInStream.TRIM_HORIZON, //consumption model TRIM_HORIZON is an earliest consumption model, LATEST is the latest consumption model
                                          Seconds(1), //frequency of consumption
                                          StorageLevel.MEMORY_AND_DISK_SER_2 //cache and persisit method
                                         )                                           
                                     
    
    // converting the byte array format from kinesis to string (deserializing)
    val finalstream = stream.map(x=>b2s(x))
    
    finalstream.print()
    
    ssc.start()
    ssc.awaitTermination()
 
    
  }
  }