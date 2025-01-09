import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, from_json, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object EcommerceTransactionsSSS{
  def main(args: Array[String]): Unit = {
    val KAFKA_TOPIC_NAME_CONS = "testtopic"
    val KAFKA_OUTPUT_TOPIC_CONS = "outputtopic"
    val KAFKA_BOOTSTRAP_SERVER_CONS = "localhost:9092"

    println("Spark Structured Streaming with kafka Demo Application Started ...")
    val spark = SparkSession
      .builder()
      .appName("Spark Structured Streaming with kafka Demo")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions","2")

    import spark.implicits._

    val transactionsDetailsDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVER_CONS)
      .option("subscribe",KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets","latest")
      .load()

    println("Printing Schema of transaction_details_df: ")
    transactionsDetailsDf.printSchema()

    val transactionsDetailsDfCasted = transactionsDetailsDf
      .selectExpr("CAST(value AS STRING)","timestamp")

    val transactionsDetailsDfSchema = StructType(List(
        StructField("transaction_id",IntegerType),
        StructField("transaction_card_type",StringType),
        StructField("transaction_amount",FloatType),
        StructField("transaction_datetime",StringType),
      ))

    val transactionsDetailsDfRenamed = transactionsDetailsDfCasted
      .select(
        from_json(
          $"value",transactionsDetailsDfSchema)
          .as("transaction_details"),
        $"timestamp")

    val transactionsDetailsDfSelected = transactionsDetailsDfRenamed
      .select("transaction_details.*", "timestamp")

    val transactionsDetailsDfAggregated = transactionsDetailsDfSelected
      .groupBy("transaction_card_type")
      .sum("transaction_amount")
      .select($"transaction_card_type",
        col("sum(transaction_amount)").
          as("total_transaction_amount")
      )

    println("Printing Schema of transactionsDetailsDfAggregated: ")
    transactionsDetailsDfAggregated.printSchema()

    val transactionDetailsDFRefactor = transactionsDetailsDfAggregated
      .withColumn("key",lit(100))
      .withColumn("value", concat(lit("{'transaction_card_type': '"),
        col("transaction_card_type"), lit("', 'total_transaction_amount': '"),
        col("total_transaction_amount").cast("string"),lit("'}")))

    println("Printing Schema of transactionDetailsDFRefactor: ")
    transactionDetailsDFRefactor.printSchema()

//    val query = transactionDetailsDFRefactor
//      .writeStream
//      .format("console")
//      .outputMode("complete")
//      .option("truncate",false)
//      .start()

    val transactionDetailsDFWriteStream = transactionDetailsDFRefactor
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER_CONS)
      .option("topic", KAFKA_OUTPUT_TOPIC_CONS)
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .outputMode("update")
      .option("checkpointLocation","check_point_loc")
      .start()

//    val query = transactionDetailsDFRefactor
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .writeStream
//      .format("console")
//      .outputMode("complete")
//      .option("truncate",false)
//      .start()

//    query.awaitTermination()
    transactionDetailsDFWriteStream.awaitTermination()
  }
}