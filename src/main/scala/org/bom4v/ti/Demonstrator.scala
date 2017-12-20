package org.bom4v.ti

//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._

// Bom4V
import org.bom4v.ti.models.customers.CustomerAccount.AccountModelForChurn
import org.bom4v.ti.models.calls.CallsModel.CallEvent
import org.bom4v.ti.serializers.customers.CustomerAccount._
import org.bom4v.ti.serializers.calls.CallsModel._
import org.bom4v.ti.data.generators.CDRFeatureProcessor

object Demonstrator extends App {

  // Spark 2.x way, with a SparkSession
  // https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("SparkSessionForBom4VDemonstrator")
    .config("spark.master", "local")
    .getOrCreate()

  // CSV data file, from the local file-system
  val cdrDataFilepath = "data/cdr/cdr_example.csv"
  // CSV data file, from HDFS
  // (check the fs.defaultFS property in the $HADOOP_CONF_DIR/core-site.xml file)
  // val cdrDataFilepath = "hdfs://localhost:8020/data/bom4v/data/cdr/cdr_example.csv"

  //
  import spark.implicits._

  // Read the raw data
  val cdr_data_t : org.apache.spark.sql.Dataset[CallEvent] = spark.read
    .schema(callEventSchema)
    .option("inferSchema", "false")
    .option("header", "true")
    .option("delimiter", "^")
    .csv(cdrDataFilepath)
    .as[CallEvent]

  //
  cdr_data_t.printSchema()
  cdr_data_t.show()

  // data preparation
  val cdr_data_transponed : org.apache.spark.sql.DataFrame = CDRFeatureProcessor.extractFields (cdr_data_t)

  //
  cdr_data_transponed.printSchema()
  cdr_data_transponed.show()

  // Stop Spark
  spark.stop()
}
