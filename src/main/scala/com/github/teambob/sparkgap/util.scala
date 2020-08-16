/*
Copyright 2020 Andrew Punch

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.github.teambob.sparkgap

import java.io.{Reader, StringReader}
import java.util.Properties

import org.apache.log4j.PropertyConfigurator

import collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import com.google.gson.{Gson, JsonObject}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ListBuffer

/**
 * Utility object
 */
object util {
  /**
   * Register all UDFs in the udf object
   *
   * @param spark Spark session to register the UDFs into
   */
  def registerAllUdfs(spark: SparkSession): Unit = {
    spark.udf.register("tryGet", udf.tryGet)
  }

  /**
   * Read JSON files. Can read JSON files where records are concatenated documents or newline delimited documents
   * It will not open where all records are in a single JSON document - use spark's option("multiLine", true) instead
   *
   * @param spark Sparksession instance
   * @param paths Paths to open. Just like normal spark json read
   * @return Dataframe containing contents of JSON file(s)
   */
  def readJson(spark: SparkSession, paths: String*): DataFrame = {
    val dfText: Dataset[String] = spark.read.textFile(paths: _*)

    val gson = new Gson()
    val dfReformatted: Dataset[String] = dfText.flatMap((x:String)=>{
      val reader: Reader = new StringReader(x)
      var outputJson = new ListBuffer[String]()
      while (reader.ready()) {
        val json: JsonObject = gson.fromJson(reader, classOf[JsonObject])
        outputJson += json.toString
      }
      outputJson.toIterator.asJava
    }, Encoders.STRING)

    spark.read.json(dfReformatted)
  }

}
