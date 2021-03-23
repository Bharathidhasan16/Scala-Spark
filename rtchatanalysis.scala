package org.inceptez.hack.streaming

import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.Column
//import org.elasticsearch.spark._
//import org.elasticsearch.spark.sql._

object rtchatanalysis {
  
  def main(args:Array[String])
  {
    //4. Create a Spark streaming app with the package org.inceptez.hack.streaming with object name as
    //rtchatanalysis to consume json data once in 20 seconds and do the followings,

    println("spark streaming")
    val spark=SparkSession.builder.appName("chatbot_application").master("local[*]").
    //config("hive.metastore.uris","thrift://localhost:9083").
    //config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse").
    //config("es.nodes","localhost").
    //config("es.port","9200").
    //config("es.index.auto.create","true").
    //config("es.mapping.id","chat").
    //enableHiveSupport().
    getOrCreate()
    
    val sc=spark.sparkContext
    val sqlc=spark.sqlContext
    val ssc=new StreamingContext(sc, Seconds(20))
    
    import spark.implicits._
    
    sc.setLogLevel("ERROR")
    
    //Kafka Topic
    val topic = Array("chatbot")
    
    //Kafka Params
    val params=Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "hackathon",
        "auto.offset.reset" -> "earliest"
        )
    
    val stream = KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](topic,params))
        
    val kafkastream=stream.map(record => record.value)
    
    
    kafkastream.foreachRDD{x=>
      println("Checking if there is data")
      if(!x.isEmpty())
      {
        //5. From the json df the id is the customer who is chatting with the support agent, chattext is the
        //plain text of the chat message, type is the indicator of c means customer or a means agent.
        println("Processing data")
        //converting to DF
        val chatdf=sqlc.read.option("multiline", "true").option("mode", "DROPMALFORMED").json(x)
        println("Data Received from Nifi")
        chatdf.show(5,false)

        
        //6. Filter only the records contains 'type' as 'c' (to filter only customer interactions)

        val chatdf1=chatdf.where("agentcustind='c'")
        val chatdfremoved=chatdf1.drop("agentcustind")

        //7.Remove the column 'type' from the above dataframe, hence the resultant dataframe contains
        //only id and chat and convert to tempview.

        chatdfremoved.createOrReplaceTempView("chattable")
        //8. Use SQL split function on the 'chat' column with the delimiter as ' ' (space) to tokenize all words

        val df=sqlc.sql("select id,split(chattext,' ') as chatarray from chattable ")
        df.createOrReplaceTempView("chattable1")
        df.show(2,false)
        
        //9. Use SQL explode function to pivot the above data created in step 8

        val df1=sqlc.sql("select id,chat from chattable1 LATERAL VIEW explode(chatarray) w as chat")
        df1.show(20,false)
        df1.createOrReplaceTempView("chattempview")
        
        //8. Load the stopwords from linux file location

        val df2=sqlc.read.csv("file:////home/hduser/SPARK_HACKATHON_2021/realtimedataset/stopwords")
        val df3=df2.withColumnRenamed("_c0","stopword")
        df3.show(10,false)
        df3.createOrReplaceTempView("stopwordtempview")
        
        //9. Write a left outer join between chat tempview created in step 9 and stopwords tempview created in
        //step 8 and filter all nulls (or) use subquery with not in option to filter all stop words from the actual chat
        //tempview using the stopwords tempview created above
        
        val df4=sqlc.sql("select a.id,a.chat from chattempview a left outer join stopwordtempview b on a.chat=b.stopword")
        df4.show(20)
        
        //10. Load the final result into a hive table should have only result as given below using append option.
        // hanging system if I use config for hive (enable hive support). so string it in mysql
        val prop=new java.util.Properties();
        prop.put("user", "root")
        prop.put("password", "root")
        prop.put("driver","com.mysql.jdbc.Driver")
    
        df4.write.mode("append").jdbc("jdbc:mysql://127.0.0.1/hackathon","matchedword", prop)
        
//**************------------------------------------------------------------------------------------------***********************
//        ******Not able to implement Elastic search in same program due to memory constraint. 
//        ******Created ES.scala to implement ES
//***************------------------------------------------------------------------------------------------*************************
        
        //12. Store the above result in an Elastic Search index chatidx and create visualization using Kibana.
        //format with column names as chatkeywords, occurance
        
        /*val df5=sqlc.read.format("jdbc").jdbc("jdbc:mysql://127.0.0.1/hackathon","matchedword", prop)
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
        //df6.saveToEs("CDC/FreqUsed")
        println("Successfully loaded to Elastic Search")*/

      }
      println("No data")
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(300000)
  }
  
}