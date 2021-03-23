package org.inceptez.hack.streaming
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.sql.Column
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

object ES {
   def main(args:Array[String])
  {
     
    val spark=SparkSession.builder.appName("application").master("local[*]").
    //config("hive.metastore.uris","thrift://localhost:9083").
    //config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse").
    config("es.nodes","localhost").
    config("es.port","9200").
    config("es.index.auto.create","true").
    config("es.mapping.id","chat").
    //enableHiveSupport().
    getOrCreate();

    val sc=spark.sparkContext
    val sqlc=spark.sqlContext
    sc.setLogLevel("ERROR")
  
        val prop=new java.util.Properties();
        prop.put("user", "root")
        prop.put("password", "root")
        prop.put("driver","com.mysql.jdbc.Driver")
  
        val df5=sqlc.read.format("jdbc").jdbc("jdbc:mysql://127.0.0.1/hackathon","matchedword", prop)
        df5.createOrReplaceTempView("freq")
        
        //11. Identify the most recurring keywords used by the customer in all the chats by grouping based on the
        //keywords used with count of keywords. use group by and count functions in the sql
        //for eg.
        //issues,2
        //notworking,2
        //tv,2
        
        println("printing most frequently used word")
        val df6=sqlc.sql("select chat,count(*) as chatcount from freq group by chat order by chatcount desc")
        df6.show()
        df6.saveToEs("cdc/FreqUsed")
        println("Successfully loaded to Elastic Search")
        
  }
}