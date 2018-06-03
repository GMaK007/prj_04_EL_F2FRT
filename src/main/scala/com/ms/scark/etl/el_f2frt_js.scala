package com.ms.scark.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

class el_f2frt_js {
  /*def flatten(schema: StructType): Array[StructField] = schema.fields.flatMap { f =>
    f.dataType match {
      case struct: StructType => flatten(struct)
      case _ => Array(f)
    }
  }*/
  def jsonParseCheck (rSS: SparkSession, rinfileNm: String): DataFrame = {
    val rawjsonSL = rSS.read.json(rinfileNm)
    if (rawjsonSL.schema.fieldNames.length == 1 && rawjsonSL.schema.fieldNames(0).split("_")(1) == "corrupt") {
      val rawjsonML = rSS.read.option("multiline", "true").json(rinfileNm)
      if (rawjsonML.schema.fieldNames.length == 1 && rawjsonML.schema.fieldNames(0).split("_")(1) == "corrupt") {
        println("Error: Unparseable JSON format | Fatal Error")
        System.exit(99)
        rawjsonML
      } else { rawjsonML }
    } else { rawjsonSL }
  }
  def flattenSchema(schema: StructType, prefix: String = null) : Array[String] =
    schema.fields.flatMap{f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(colName)
      }
    }
    }
  def invo_validateSchema_json(rSS: SparkSession, rinfileNm: String): Unit = {
    //val sc1 = rSS.sparkContext
    //val jRDD = sc1.textFile(rinfileNm).collect
    println ("JSON Schema Validation is not supported yet...Keep Checking!!!")
  }
}

