package Combined

import Combined.JoinDataFrames._
import Combined.argument.isArgsValid
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataExtract {
  val spark: SparkSession = SparkSession.builder()
    .appName("CX Transformation Application")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.inMemoryColumnarStorage.compressed",true)
    .config("spark.sql.adaptive.coalescePartitions.enabled",true)
    .config("spark.dynamicAllocation.maxExecutors",8)
    .config("park.yarn.executor.memoryOverhead",1024)
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    if (!isArgsValid(args)) {
      throw new Exception("Arguments passed are invalid...")
    }

    val Transactionparth = args(0)
    val custarnspath = args(1)
    val accountparth = args(2)

    val Reporting = args(3)
    //val TableNameCX = args(4)

    val TransactionTable =spark.read.parquet(Transactionparth)
    val xTrans = TransactionTable.cache()
    val CustomerTable = spark.read.parquet(custarnspath)
    val xCust = CustomerTable.cache()
    val AccountTable = spark.read.parquet(accountparth)
    val xAcc = AccountTable.cache()
    val CustFinal = Customer(xTrans,xCust, xAcc)
    CustFinal.printSchema()
    import spark.implicits._
     CustFinal.repartition(20).write.option("header",true).mode(SaveMode.Overwrite).partitionBy("cif_customer_key","partitioned_date").parquet(Reporting)

  }

}