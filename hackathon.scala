package org.bharathi.sparkhack

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.sql.Date
import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min}
    
//case class insureclass1(IssuerId1:Int,IssuerId2:Int,BusinessDate: Date,StateCode: String,SourceName: String,NetworkName: String,
//    NetworkURL: String, custnum: Int,MarketCoverage: String,DentalOnlyPlan: String)
  case class insureclass(IssuerId1:Int,IssuerId2:Int,BusinessDate: String,StateCode: String,SourceName: String,NetworkName: String,
      NetworkURL: String, custnum: Int,MarketCoverage: String,DentalOnlyPlan: String)
      

        
object hackathon 
{
  def main(args:Array[String])
  {
    println("Inceptez Hackathon 2021 - Bharathidhasan")
    
    
    val spark=SparkSession.builder.appName("Hackathon").master("local[*]").
    config("hive.metastore.uris","thrift://localhost:9083").
    config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse").
    enableHiveSupport().getOrCreate();
    
    
    val sc=spark.sparkContext
    sc.setLogLevel("error")
    val sqlc=spark.sqlContext
    val stc=new StreamingContext(sc,Seconds(10))
    
    sc.setLogLevel("error")
 
//-------------------------------------------------------
//1. Data cleaning, cleansing, scrubbing (20% Completion)
//-------------------------------------------------------
    
//1. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata
    
    val insuredata=sc.textFile("hdfs://127.0.0.1:54310/user/hduser/sparkhack2/insuranceinfo1.csv")
    val part=insuredata.partitions.size
    println("partition ",part)
    
//2. Remove the header line from the RDD contains column names.
    val HeadCount=insuredata.count
    println ("Count before removing Header & Trailer(Original Count) ",HeadCount)
    
    val header = insuredata.first()
    val insuredatanohead=insuredata.filter(x=>x!=(header))
   // val check = insuredatanohead.take(10)
   // check.foreach(println)
    val HeadCount1=insuredatanohead.count
    println("Count after removing Header ",HeadCount1)

//3. Remove the Footer/trailer also which contains “footer count is 404”
    
  /*val Trailer=insuredatanohead.zipWithIndex()
    val Trailcount=Trailer.count
    println("Count before removing trailer ",Trailcount)
    val insuredatanoHT=Trailer.filter(x=>x._2 < Trailcount -1)
    
    val Trailcount1=insuredatanoHT.count
    println("Count before after Trailer ",Trailcount1)
    
    val data=insuredatanoHT.take(5)
    data.foreach(println)*/
    
    val insuredatanoHT=insuredatanohead.filter(x => !x.contains("footer count is 402"))
    val count=insuredatanoHT.count
    println ("Count after removing trailer ", count)
    

//4. Display the count and show few rows and check whether header and footer is removed.
    
    println("Printing the data after removing header & trailer")
    val read=insuredatanoHT.take(10)
    read.foreach(println)
    
//5. Remove the blank lines in the rdd.
    
    val insurenoblank=insuredatanoHT.map(x=>x.trim()).filter(x=>x.length() != 0)
    val cnt=insurenoblank.count
    println("count after removing blank lines",cnt)
    val len1=insurenoblank.take(10)
    println("Printing after removing blank lines")
    len1.foreach(println)
    
//6. Map and split using ‘,’ delimiter.
    
    val insuresplit = insurenoblank.map(x=>x.split(","))
    
//7. Filter number of fields are equal to 10 columns only - analyze why we are doing this and provide your view here..
    
    val insurecol=insuresplit.filter(x => x.length == 10)
    val cnt1=insurecol.count
    println("Count of fields equal to 10 columns", cnt1)
    
//8. Add case class namely insureclass with the field names used as per the header record in
//   the file and apply to the above data to create schemaed RDD.
  
    val Schemaedinsurecol=insurecol.map(x=>insureclass(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7).toInt,x(8),x(9))) // Date.valueOf(x(2))
    val s= Schemaedinsurecol.partitions.size
    println("Partition size", s)
    val r=Schemaedinsurecol.repartition(4)
    val re=r.partitions.size
    println("Re-Partition size", re)
    println("Printing the Schemaed RDD")
    Schemaedinsurecol.foreach(println)
    val v=Schemaedinsurecol.count
    println("Schema Rdd count",v)
    
//9. Take the count of the RDD created in step 7 and step 1 and print how many rows are
//   removed/rejected in the cleanup process of removing fields does not equals 10.
    
    println ("Original Count in step 1 ",HeadCount)
    println("Count in step 7 i.e, fields equal to 10 columns", cnt1)
    println("Total records removed/rejected in the cleanup process ", HeadCount-cnt1)

//10. Create another RDD namely rejectdata and store the row that does not equals 10 columns. 
//    With a new column added in the first column called numcols contains number
//    of columns in the given row For eg - the output should be like this: (6,219) where 6 is
//    the number of columns and 219 is the first column IssuerId of the data hence
//    we can analyse the IssuerIds contains deficient fields.
    import sqlc.implicits._
    
    val rejectdataraw=insuresplit.filter(x => x.length != 10)//.toDF()
    //val order=rejectdataraw.orderBy("IssuerId1")
    //val rejectdata=rejectdataraw.withColumn("numcols", row_number().over(order))
    //rejectdata.show(4)

//11. Load the file2 (insuranceinfo2.csv) from HDFS using textFile API into an RDD insuredata2
    
    val insuredata2=sc.textFile("hdfs://127.0.0.1:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
 
    
//12. Repeat from step 2 to 8 for this file also and create the schemaed rdd from the
//    insuranceinfo2.csv and filter the records that contains blank or null IssuerId,IssuerId2
//    for eg: remove the records with pattern given below.
//    ,,,,,,,13,Individual,Yes
    
    
    val HeadCnt=insuredata2.count
    println ("File2: Count before removing Header & Trailer(Original Count) ",HeadCnt)
    
    val header1 = insuredata2.first()
    val insuredatanohead1=insuredata2.filter(x=>x!=(header1))
   // val check = insuredatanohead.take(10)
   // check.foreach(println)
    val HeadC1=insuredatanohead1.count
    println("File2: Count after removing Header ",HeadC1)
    
    val insuredatanoHT1=insuredatanohead1.filter(x => !x.contains("footer count is 3333"))
    val count1=insuredatanoHT1.count
    println ("File2: Count after removing trailer ", count1)

    
    println("File2: Printing the data after removing header & trailer")
    val read1=insuredatanoHT1.take(10)
    read1.foreach(println)
    
    val insurenoblank1=insuredatanoHT1.map(x=>x.trim()).filter(x=>x.length() != 0)
    val cnt2=insurenoblank1.count
    println("File2: count after removing blank lines",cnt2)
    val len2=insurenoblank1.take(10)
    println("File2: Printing after removing blank lines")
    len2.foreach(println)
    
    val insuresplit1 = insurenoblank1.map(x=>x.split(","))
    
    val insurecol1=insuresplit1.filter(x => x.length == 10)
    val cnt3=insurecol1.count
    println("File2: Count of fields equal to 10 columns is", cnt3)

    val Schemaedinsurecol1=insurecol1.map(x=>insureclass(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7).toInt,x(8),x(9)))
    val read2=Schemaedinsurecol1.take(5)
    read2.foreach(println)
    //val b=Schemaedinsurecol1.count // Get error here
    //println("here",b)

    val insurenotnullFile1=Schemaedinsurecol.filter(x=>(x.IssuerId1 != null)).filter(x=>(x.IssuerId2 != null))
    val insurenotnullFile2=Schemaedinsurecol1.filter(x=>(x.IssuerId1 != null)).filter(x=>(x.IssuerId2 != null))
   
    
//----------------------------------------------------------------------------------------------------
//2. Data merging, Deduplication, Performance Tuning & Persistance (15% Completion) – Total 35%
//----------------------------------------------------------------------------------------------------
 
//13. Merge the both header and footer removed RDDs derived in steps 8 and 12 into an RDD namely insuredatamerged
    
    val insuredatamerged=insurenotnullFile1.union(insurenotnullFile2)
    
//14. Persist the step 13 RDD to memory by serializing.
    
    insuredatamerged.cache()
    
//15. Calculate the count of rdds created in step 8+12 and rdd in step 13, check whether they are matching.
    
    println(insurenotnullFile1.count)
    //val c1=insurenotnullFile1.count
    //println(c1)
    //val c0=insuredatamerged.count
    //println(c0)
//16. Remove duplicates from this merged RDD created in step 13 and print how many duplicate rows are there.
    
    //val insuredatamergedd=insuredatamerged.distinct
    //val show=insuredatamergedd.take(2)
    //println(show)
    
    
//17. Increase the number of partitions in the above rdd to 8 and name it as insuredatarepart.
    
    val partinsuredatamerged=insuredatamerged.repartition(8)
    val partition1 = partinsuredatamerged.partitions.size
    println("Size of Partition is ",partition1)
    //val d=partinsuredatamerged.take(30)
    //println("take",d)
    
//18. Split the above RDD using the businessdate field into rdd_20191001 and rdd_
//20191002 based on the BusinessDate of 2019-10-01 and 2019-10-02 respectively
    
    val rdd_20191001=insurenotnullFile1.filter { x => x.BusinessDate == "2019-10-01"}
    val rdd_20191002=insurenotnullFile1.filter { x => x.BusinessDate == "2019-10-02"}
    
   // println("rdd_20191001 count",rdd_20191001.count)
   // println("rdd_20191002 count",rdd_20191002.count)
    
//19. Store the RDDs created in step 10, 13, 18 into HDFS locations.
    
   rejectdataraw.saveAsTextFile("hdfs://localhost:54310//user/hduser/sparkhack2/output/RDDOut/")
    
//20. Convert the RDD created in step 17 above into Dataframe namely insuredaterepartdf using toDF function
    
    //val partinsuredatamergedDF=partinsuredatamerged.toDF()
    //val b=partinsuredatamergedDF.count()
    //println("bharathi",b)

    val partinsuredatamergedDF=insurenotnullFile1.toDF();
//--------------------------------------------------------------------    
//    3. DataFrames operations (20% Completion) – Total 55%
//--------------------------------------------------------------------    
    
    //IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan

//21. Create structuretype for all the columns as per the insuranceinfo1.csv with the columns 
    
    def structforDF=StructType(Array(StructField("IssuerId",IntegerType,true),StructField("IssuerId2",IntegerType,true),
    StructField("BusinessDate",DateType,true),StructField("StateCode",StringType,true),StructField("SourceName",StringType,true),
    StructField("NetworkName",StringType,true),StructField("NetworkURL",StringType,true),StructField("custnum",IntegerType,true),
    StructField("MarketCoverage",StringType,true),StructField("DentalOnlyPlan",StringType,true)));

//22. Create dataframes using the csv module with option to escape ‘,’ accessing the insuranceinfo1.csv and insuranceinfo2.csv 
//   files and remove the footer from both dataframes using header true, dropmalformed options and apply the schema of the
//   structure type created in the step 21.
      
    val df1=spark.read.option("header","true").option("escape",",").option("mode","dropmalformed").
    schema(structforDF).csv("hdfs://127.0.0.1:54310/user/hduser/sparkhack2/insuranceinfo1.csv",
    "hdfs://127.0.0.1:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
    
    println(df1.count)
    df1.show(10,false)

//23. Apply the below DSL functions in the DFs created in step 22.
//a. Rename the fields StateCode and SourceName as stcd and srcnm respectively.
//b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field
//Hint : Cast to string and concat.
//c. Remove DentalOnlyPlan column
//d. Add columns that should show the current system date and timestamp with the fields name of sysdt and systs respectively.
    
    val df2=df1.withColumnRenamed("StateCode","stcd").
    withColumnRenamed("SourceName","srcnm").
    withColumn("IssuerId",$"IssuerId".cast("String")).
    withColumn("IssuerId2",$"IssuerId2".cast("String")).
    withColumn("issueridcomposite",concat($"IssuerId",$"IssuerId2")).
    withColumn("IssuerId",$"IssuerId".cast("Int")).
    withColumn("IssuerId2",$"IssuerId2".cast("Int")).
    drop($"DentalOnlyPlan").
    withColumn("sysdt",current_date()).//.as("Curent_Date")
    withColumn("systs",current_timestamp())
    df2.printSchema()
    df2.show(10)

//Try the below usecases also seperately:
//i. Identify all the column names and store in an array variable – use columns function.
    val cols=df2.columns  // Array[String]
    val arr=Array(cols) 
    //println(arr)
    
//ii. Identify all columns with datatype and store in an array variable – use dtypes function.

    val types=df2.dtypes  // Array[String,String]
    //val arr1=Array(types)
    
//iii. Identify all integer columns alone and store in an array variable.
    
    val iden=types.filter(x=>x._2=="IntegerType").map(x=>x._1)
    //val iden=arr1.map(x=>x.filter(y=>y._2=="IntegerType").map(x=>x._1))

    val str=iden.mkString(",")
    //val iden=arr1.map(x=>x.filter(y=>y._2 == "IntegerType").map(z=>z._1))
    
//iv. Select only the integer columns identified in the above statement and print 10 records in the screen.
    
    //df2.select(str).show(10)=================================== Not able to display
    
//24. Take the DF created in step 23.d and Remove the rows contains null in any one of the 
//field and count the number of rows which contains all columns with some value.
    
    val df3=df2.na.drop;
    println(df2.count())
    println(df3.count)
    
//25. Custom Method creation: Create a package (org.inceptez.hack), class (allmethods), method (remspecialchar)
//Hint: First create the function/method directly and then later add inside pkg, class etc..
//a. Method should take 1 string argument and 1 return of type string
//b. Method should remove all special characters and numbers 0 to 9 - ? , / _ ( ) [ ]
//Hint: Use replaceAll function, usage of [] symbol should use \\ escape sequence.
//c. For eg. If I pass to the method value as Pathway - 2X (with dental) it has to return Pathway X with dental as output.
    
    
    import org.bharathi.sparkhack.allmethods
    
    val speccharobj=new allmethods;
    
    println("Checking method ",speccharobj.remspecialchar("1bh}^@%!*()ar&ath?12i["))
    
//26. Import the package, instantiate the class and register the method generated in step 25
//as a udf for invoking in the DSL function.
    
    val dfudf=udf(speccharobj.remspecialchar _)
    //val dfudf=spark.udf.register("sqludf",speccharobj.remspecialchar _)
    
//27. Call the above udf in the DSL by passing NetworkName column as an argument to get the special characters removed DF.
    
    val df4=df3.select($"IssuerId",$"IssuerId2",$"BusinessDate",$"stcd",$"srcnm",
        dfudf($"NetworkName").alias("CleanNetworkName"),$"NetworkURL",$"custnum",$"MarketCoverage",$"issueridcomposite",$"sysdt",$"systs")
        df4.show(5)
        
        //df3.select($"NetworkName")show(20,false)
        //df4.select($"UDF(NetworkName)")show(20,false)
    
//28. Save the DF generated in step 27 in JSON into HDFS with overwrite option.
     
    df4.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/sparkhack2/output/dataframe.json")
        
//29. Save the DF generated in step 27 into CSV format with header name as per the DF and delimited by ~ into HDFS with overwrite option.
    
    df4.write.option("header","true").mode("overwrite").option("delimiter","~").csv("hdfs://localhost:54310/user/hduser/sparkhack2/output/dataframe.csv")
    
//30. Save the DF generated in step 27 into hive external table and append the data without overwriting it.
    
    //df4.createOrReplaceTempView("df4table")
    
    spark.sql("create database if not exists hackathon")
    spark.sql("use hackathon")
    //spark.sql("drop table if exists dfhackathontable");
    spark.sql("""create external table if not exists hackathon.dfhackathontable (IssuerId int,IssuerId2 int,BusinessDate date,stcd varchar(10),srcnm varchar(20),
      NetworkName String,NetworkURL String,custnum int,MarketCoverage String,issueridcomposite bigint, sysdt date,systs string) 
      row format delimited fields terminated by ',' location 'hdfs://localhost:54310/user/hduser/sparkhack2/output/DFTable'""") 
      
    df4.write.mode(SaveMode.Append).saveAsTable("hackathon.dfhackathontable") //SaveMode.Append
    
//-------------------------------------------------------------------------------
//    4. Tale of handling RDDs, DFs and TempViews (20% Completion) – Total 75%
//----------------------------------------------------------------------------------
    
//31. Load the file3 (custs_states.csv) from the HDFS location, using textfile API in an RDD custstates, this file contains 
//2 type of data one with 5 columns contains customer master info and other data with statecode and description of 2 columns.
    
    val custstates=sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
    
//32. Split the above data into 2 RDDs, first RDD namely custfilter should be loaded only with5 columns data and second RDD 
//namely statesfilter should be only loaded with 2 columns data.
    
    val custfilter=custstates.map(x=>x.split(",")).filter(x=>x.length==5)
    val statesfilter=custstates.map(x=>x.split(",")).filter(x=>x.length==2)
    custfilter.count
    statesfilter.count
    
//Use DSL functions:
//--------------------
//33. Load the file3 (custs_states.csv) from the HDFS location, using CSV Module in a DF custstatesdf, this file contains 2 
//type of data one with 5 columns contains customer master info and other data with statecode and description of 2 columns.
    val custstatesdf=spark.read.csv("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
    custstatesdf.show(2)
    println(custstatesdf.count)
    
//34. Split the above data into 2 DFs, first DF namely custfilterdf should be loaded only with 5 columns data and second DF 
//namely statesfilterdf should be only loaded with 2 columns data.
//Hint: Use filter/where DSL function to check isnull or isnotnull to achieve the above functionality then rename, change the 
//type and drop columns in the above 2 DFs accordingly.
    
    val custfilterdf=custstatesdf.na.drop(Array("_c2","_c3","_c4"))
    println(custfilterdf.count)
    //custfilterdf.printSchema
    
    val statesfilterdf=custstatesdf.where("_c2 Is Null").where("_c3 Is Null").where("_c4 Is Null").drop("_c2","_c3","_c4")
    println(statesfilterdf.count)
    //statesfilterdf.printSchema
    
    val custfilterdf1=custfilterdf.
    withColumn("_c0",$"_c0".cast("int")).
    withColumn("_c3",$"_c3".cast("int")).
    withColumnRenamed("_c0","custid").
    withColumnRenamed("_c1","fname").
    withColumnRenamed("_c2","lname").
    withColumnRenamed("_c3","age").
    withColumnRenamed("_c4","prof")
    
    val statesfilterdf1=statesfilterdf.
    withColumnRenamed("_c0","statecode").
    withColumnRenamed("_c1","state")
    
    custfilterdf1.show(2)
    custfilterdf1.printSchema
    
    statesfilterdf1.show(2)
    statesfilterdf1.printSchema
    
   // withColumn("IssuerId",$"IssuerId".cast("String")).
    
//Use SQL Queries:
//35. Register the above step 34 DFs as temporary views as custview and statesview.
    
    custfilterdf1.createOrReplaceTempView("custview")
    statesfilterdf1.createOrReplaceTempView("statesview")
    
//36. Register the DF generated in step 23.d as a tempview namely insureview
    
    df2.createOrReplaceTempView("insureview")
    
//37. Import the package, instantiate the class and Register the method created in step 25 in the name of remspecialcharudf using spark udf registration.
    
    //import org.bharathi.sparkhack.allmethods 
    //val speccharobj=new allmethods;
    //val dfudf=udf(speccharobj.remspecialchar _)
    val remspecialcharudf=spark.udf.register("remspecialcharudf",speccharobj.remspecialchar _)

    
//38. Write an SQL query with the below processing – set the spark.sql.shuffle.partitions to 4
    
    sqlc.setConf("spark.sql.shuffle.partitions","4");
    
//a. Pass NetworkName to remspecialcharudf and get the new column called cleannetworkname
//b. Add current date, current timestamp fields as curdt and curts.
//c. Extract the year and month from the businessdate field and get it as 2 new fields called yr,mth respectively.
//d. Extract from the protocol either http/https from the NetworkURL column, if http then print http non secured if https then 
//secured else no protocol found then display noprotocol. For Eg: if http://www2.dentemax.com/ then show http non
//secured else if https://www2.dentemax.com/ then http secured else if www.bridgespanhealth.com then show as no protocol 
//store in a column called protocol.
    
    //df2.show(5)
    val df3sql=sqlc.sql("""select IssuerId,IssuerId2,BusinessDate,stcd,srcnm,remspecialcharudf(NetworkName) as CleanNetworkName,
                      NetworkURL, case when NetworkURL like 'https:%' then "Secured_connection"
                      when NetworkURL like 'http:%' then "Non_Secured_connection"
                      else "No_Protocol" end as Protocol,
                      custnum,MarketCoverage,sysdt,systs,year(BusinessDate) as yr,month(BusinessDate) as mth from insureview""")
    df3sql.show(10)
    
    df3sql.createOrReplaceTempView("insureview1")
   
//e. Display all the columns from insureview including the columns derived from above a, b, c, d steps with statedesc column 
//from statesview with age,profession column from custview . Do an Inner Join of insureview with statesview using
//stcd=stated and join insureview with custview using custnum=custid.
    
    val df3sql1=sqlc.sql("""select T1.IssuerId,T1.IssuerId2,T1.BusinessDate,T1.stcd,T1.srcnm,T1.CleanNetworkName,T1.NetworkURL,T1.Protocol,
                      T1.custnum,T1.MarketCoverage,T1.sysdt,T1.systs,T1.yr,T1.mth,T2.state,T3.age,T3.prof from insureview1 T1 inner join statesview T2 on
                      T1.stcd=T2.statecode inner join custview T3 on T1.custnum=T3.custid""")
    df3sql1.show(5)
    
//39. Store the above selected Dataframe in Parquet formats in a HDFS location as a single file.
    
     df3sql1.coalesce(1).write.mode("overwrite").parquet("hdfs://localhost:54310/user/hduser/sparkhack2/output/dataframe.parquet")
    
//40. Write an SQL query to identify average age, count group by statedesc, protocol, profession including a seqno column added 
//which should have running sequence number partitioned based on protocol and ordered based on count descending and
//display the profession whose second highest average age of a given state and protocol.
//For eg.
//Seqno,Avgage,count,statedesc,protocol,profession
//1,48.4,10000, Alabama,http,Pilot
//2,72.3,300, Colorado,http,Economist
//1,48.4,3000, Atlanta,https,Health worker
//2,72.3,2000, New Jersey,https,Economist
    
    df3sql1.createOrReplaceTempView("insureview2")
    
    val df3sql2=sqlc.sql("""select row_number() over(partition by protocol order by protocol) as Seqno,avg(age) as Avgage,
                         count(1) as count,state,Protocol,prof from insureview2 group by state,Protocol,prof order by Seqno""")
    df3sql2.show(30)
    println(df3sql2.count)
                         
//41. Store the DF generated in step 39 into MYSQL table insureaggregated.
    
    val prop=new java.util.Properties();
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver","com.mysql.jdbc.Driver")
    
    df3sql2.write.mode("overwrite").jdbc("jdbc:mysql://127.0.0.1/hackathon","insureaggregated", prop)
    
//42. Try Package the code using maven install, take the lean and fat jar and try submit with driver memory of 512M, number of 
//executors as 4, executor memory as 1GB and executor cores with 2 cores. Try Dynamic allocation also.

    //spark-submit --class org.bharathi.sparkhack.hackathon 
    //--master local[2] /home/hduser/workspacespark/Hackathon/target/Hackathon-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
    //--driver-memory 512m --num-executors 1 --executor-cores 2 --executor-memory 512m
    
  }
}