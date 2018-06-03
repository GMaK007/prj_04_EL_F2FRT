package com.ms.scark.etl

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import scala.io.Source
import java.util.{Calendar, Properties}
import java.io.File
import java.io.FileNotFoundException
import com.typesafe.config.ConfigFactory

class el_f2frt_mn_v2 {
  def argsCheck(rxMode: String, rregn: String): Unit = {
   if (((rxMode != "-i") && (rxMode != "-b")) || ((rregn != "dev") && (rregn != "test") && (rregn != "prod"))) {
     println("Error: Incorrect Arguments Passed")
     println("Usage: Application.jar -<ExecMode> <Region> <InputFileNamewithPath> ")
     println("Usage: Ex: Application.jar -i dev /path/infile.txt")
     println("Usage: Allowed Values: ExecMode -i or -b")
     println("Usage: Allowed Values: Region dev or test or prod")
     System.exit(99)
   }
  }
  def fileCheck(rf: String, rinff: String): Unit = {
    if ((rinff != "Delimited") && (rinff != "JSON")) {
      println("Error: Unsupported File format specified in the properties file: Field - in.file.format")
      println("INFO: Supported formats are Delimited & JSON")
      System.exit(99)
    }
    val fl = new File(rf)
    try {
      val fc = Source.fromFile(fl); fc.close()
    } catch {
      case e: FileNotFoundException => println(s"Error: Input or Reference File - $fl Not Found"); System.exit(99)
    }
  }
  def invo_validateSchema_delim(rinfileNm: String, rregn: String): Unit = {
    //val obj1 = new el_f2frt_mn_v2
    val appConf = ConfigFactory.load()
    fileCheck(appConf.getConfig(rregn).getString("in.ref.schema"),appConf.getString("in.file.format"))
    val supSchema = firstLine(appConf.getConfig(rregn).getString("in.ref.schema")).toString.split(appConf.getConfig(rregn).getString("in.file.delimiter"))
    val infSchema = firstLine(rinfileNm).toString().split(appConf.getConfig(rregn).getString("in.file.delimiter"))
    if (!validateSchema(supSchema, infSchema))
    {
      println("Schema Validation failed")
      if (infSchema.length == supSchema.length) {
        println(s"Input File & Reference Schema has ${supSchema.length} fields but Order of the fields does not match")
      } else {
        println(s"Input File has ${infSchema.length} fields while Reference Schema has ${supSchema.length} fields")
      }
      System.exit(98)
    }
  }
  def validateSchema (rsupSchema: Array[String], rinfSchema: Array[String]): Boolean = {
    rsupSchema.sameElements(rinfSchema)
  }
  def firstLine(f: String): Option[String] = {
    val fl = new File(f)
    val src = Source.fromFile(fl)
    try { src.getLines.find(_ => true) } finally { src.close() }
  }
  def defFields (rSS: SparkSession, rinfileNm: String): DataFrame = {
    val sc = rSS.sparkContext
    val TODAY = java.time.LocalDate.now
    val defVRowRDD = sc.parallelize(Seq(Row("NA", "", -1, rinfileNm.split("/").last, TODAY.toString())))
    val defHRowRDD = StructType(Seq(
    StructField("NA", StringType, nullable = false),
    StructField("BLANK", StringType, nullable = false),
    StructField("mONE", IntegerType, nullable = false),
    StructField("IN_FILENM", StringType, nullable = false),
    StructField("PRCS_DT", StringType, nullable = false)))
    rSS.createDataFrame(defVRowRDD, defHRowRDD)
  }
}

object el_f2frt_mn_v2 {
  def main(args: Array[String]): Unit = {

    if (args.length != 3)
    {
      println("Error: Insufficient Arguments Passed")
      println("Usage: Application.jar -<ExecMode> <Region> <InputFileNamewithPath> ")
      println("Usage: Ex: Application.jar -i dev /path/infile.txt")
      println("Usage: Allowed Values: ExecMode -i or -b")
      println("Usage: Allowed Values: Region dev or test or prod")
      System.exit(99)
    }
    val xMode = args(0)
    val regn = args(1)
    val infileNm = args(2)

    val appConf = ConfigFactory.load()
    val defClmns = Array("NA", "BLANK", "mONE", "IN_FILENM", "PRCS_DT")

    val inff = appConf.getString("in.file.format")
    val JSON = if (inff == "JSON") {true} else {false}
    val TABLE = if (appConf.getString("out.type") == "T") {true} else {false}
    val BTCH = if (xMode == "-i") {false} else {true}

    //declaring the object
    val cObj = new el_f2frt_mn_v2
    val jObj = new el_f2frt_js

    //Check if Input File Format supported & File Exists
    cObj.argsCheck(xMode,regn)
    cObj.fileCheck(infileNm, inff)

    val spark = SparkSession
      .builder().config("spark.sql.warehouse.dir", appConf.getConfig(regn).getString("spark.sql.warehouse.dir"))
      .master(appConf.getConfig(regn).getString("deploymentMaster"))
      .appName("EL File to File or Table")
      .getOrCreate;
    import spark.implicits._

    if (appConf.getString("in.validateSchema") == "Y") {
      if (!JSON) {
        cObj.invo_validateSchema_delim(infileNm, regn)
      } else {
        jObj.invo_validateSchema_json(spark, infileNm)
      }
    }

    //delimit starts here
    val infClmns = {
      if (!JSON) {
        //cObj.firstLine(infileNm).toString().split(appConf.getConfig(regn).getString("in.file.delimiter"))
        spark.read.format("csv").option("delimiter", appConf.getConfig(regn).getString("in.file.delimiter")).option("header", "true").load(infileNm).columns
      } else {
        jObj.flattenSchema(jObj.jsonParseCheck(spark, infileNm).schema,"").map(x => x.stripPrefix("."))
      }
    }

    val allClmns = (infClmns ++ defClmns).map(x => (1, x)).toList.toDF("TID", "ClmnNM")
    val wSpec = Window.orderBy("TID").rowsBetween(Long.MinValue, 0)
    val allClmnsIDX = allClmns.withColumn("ClmnID", sum(allClmns("TID")).over(wSpec))

    val reqClmnsIDX = if (!BTCH) {

      if (JSON) {
        println("\n**********************************************************************")
        println("************************ INPUT JSON SCHEMA ***************************")
        println("**********************************************************************\n")
        jObj.jsonParseCheck(spark, infileNm).printSchema
        println("**********************************************************************\n")
      }
      println("\n**********************************************************************")
      println("******** Below are the List of Available Columns from InFile *********")
      println("**********************************************************************\n")

      if (allClmnsIDX.count() > 20) {
        allClmnsIDX.select("ClmnID", "ClmnNM").collect().foreach(println)
      } else {
        allClmnsIDX.select("ClmnID", "ClmnNM").show()
      }

      println("********* Please select the required columns by their ClmnID *********")
      println("*** Specify that in the right order you prefer separated by Commas ***")
      println("***** E.g. To select columns with ID 3,4,5,2,10 type 3,4,5,2,10 ******\n")

      scala.io.StdIn.readLine().split(",").map(x => (1, x)).toList.toDF("rTID", "rClmnID")

    } else {

      appConf.getConfig(regn).getString("out.file.reqColumnOrder").split(",").map(x => (1, x)).toList.toDF("rTID", "rClmnID")

    }

    val rSpec = Window.orderBy("rTID").rowsBetween(Long.MinValue, 0)
    val reqClmns = reqClmnsIDX.withColumn("ClmnSeqID", sum(reqClmnsIDX("rTID")).over(rSpec))
    val reqClmnsID = reqClmns.join(allClmnsIDX, col("rClmnID") === col("ClmnID"), "inner")
    val sClmnNms = reqClmnsID.select(col("ClmnNm")).orderBy(col("ClmnSeqID")).collect().map(_.getString(0)).map(_.replaceAll("\"", ""))

    val filedf = if (!JSON) {
      spark.read.format("csv").option("delimiter", appConf.getConfig(regn).getString("in.file.delimiter")).option("header", "true").load(infileNm)
    } else {
      jObj.jsonParseCheck(spark, infileNm)
    }
    val defDF = cObj.defFields(spark,infileNm)

    val fulldf = filedf.crossJoin(defDF)

    var new_sClmnNms = scala.collection.mutable.Buffer[String]()
    for (i <- 0 until sClmnNms.length) { new_sClmnNms += (sClmnNms(i)+"_"+(i+1).toString)}

    val findf = if (sClmnNms.distinct.length == new_sClmnNms.length) {
      fulldf.select(sClmnNms.head, sClmnNms.tail: _*)
    } else {
      fulldf.select(sClmnNms.head, sClmnNms.tail: _*).toDF(new_sClmnNms: _*)
    }

    if (!BTCH) {
      println("\n************** This is how your output would look like ***************\n")
      findf.show(1)
    }
    //findf.show(false)

    if (TABLE) {
      /***** Value declaration for DB Connection *****/
      val jdbcUsername = appConf.getConfig(regn).getString("out.db.jdbcUsername")
      val jdbcPassword = appConf.getConfig(regn).getString("out.db.jdbcPassword")
      val jdbcHostname = appConf.getConfig(regn).getString("out.db.jdbcHostname")
      val jdbcPort = appConf.getConfig(regn).getString("out.db.jdbcPort")
      val jdbcDatabase = appConf.getConfig(regn).getString("out.db.jdbcDatabase")
      val jdbcDriver = appConf.getConfig(regn).getString("out.db.jdbcDriver")
      val jdbcDBPlatform = appConf.getConfig(regn).getString("out.db.jdbcDBPlatform")

      //val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};loginTimeout=60"
      //val jdbc_url = s"${jdbcDBPlatform}${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};loginTimeout=60"
      val jdbc_url = s"${jdbcDBPlatform}${jdbcHostname}"

      val connectionProperties = new Properties()
      connectionProperties.put("user", s"${jdbcUsername}")
      connectionProperties.put("password", s"${jdbcPassword}")
      //connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      connectionProperties.put("driver", s"${jdbcDriver}")
      /************************************************/
      val sqlTableDF = spark.read.jdbc(jdbc_url, appConf.getConfig(regn).getString("out.db.jdbcTable"), connectionProperties)

      val sqlTableClmns = sqlTableDF.columns
      val finaldf = findf.toDF(sqlTableClmns: _*)
      finaldf.write.mode(SaveMode.Append).jdbc(jdbc_url, appConf.getConfig(regn).getString("out.db.jdbcTable"), connectionProperties)
    } else {
      val tgtfilepath = appConf.getConfig(regn).getString("out.file.path")
      val now = Calendar.getInstance().getTime().toString().split(" ")
      val ts = now(2) + now(1) + now(5) + "_" + now(3).replaceAll(":", "")
      val outFileNm = tgtfilepath + appConf.getConfig(regn).getString("out.file.name") + ts
      findf.write.format("csv").option("delimiter", appConf.getConfig(regn).getString("out.file.delimiter")).option("header", "true").save(outFileNm)
    }

    spark.stop()
  }
}
