package Combined


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JoinDataFrames {
  val spark: SparkSession = SparkSession.builder()
    .appName("CX Transformation Application")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat",true)
    .config("spark.sql.inMemoryColumnarStorage.compressed",true)
    .config("spark.sql.adaptive.coalescePartitions.enabled",true)
    .config("spark.dynamicAllocation.maxExecutors",8)
    .config("park.yarn.executor.memoryOverhead",1024)
    .getOrCreate()

   def Customer(Transaction: DataFrame,Customer: DataFrame,Account: DataFrame ): DataFrame = {
    import spark.implicits._
    /*val x = Transaction.withColumn("dummy",monotonically_increasing_id()% 1000)
      .withColumn("dimesion2_sufix",concat(col("customer_key"),lit("-"),col("dummy")))
    val y =Customer.withColumn("dummy",monotonically_increasing_id()% 1000)
      .withColumn("dimesion2_sufix1",concat(col("customer_key"),lit("-"),col("dummy")))
      .drop("customer_key")
    val b =Account.withColumn("dummy",monotonically_increasing_id()% 1000)
      .withColumn("dimesion2_sufix2",concat(col("customer_key"),lit("-"),col("dummy")))
      .drop("customer_key")*/
    val xCustomer = Customer.withColumn("c1",lit($"cif_customer_key")).drop("cif_customer_key")

    val xAccount = Account.withColumn("c2",lit($"cif_customer_key")).drop("cif_customer_key")

    val custFinal = Transaction.join(xCustomer,trim(Transaction("cif_customer_key"))=== trim(xCustomer("c1")),"fullouter")
       .drop(xCustomer("c1"))
       .drop(Transaction("ACCOUNT_NUMBER"))//.drop("dummy").drop(Customer("dummy"))
    custFinal.printSchema()
    val finalTB = custFinal.join(xAccount,trim(custFinal("cif_customer_key")) === trim(xAccount("c2")),"Left")
        .drop(custFinal("cif_customer_key")).drop(xAccount("SUB_PRODUCT_CODE"))
      .drop(xAccount("PRODUCT_CODE")).drop(xAccount("c2"))
    finalTB.printSchema()
    finalTB
      //.drop(Account("dummy"))

    /* val FinalCX = custFinal.join(Account,custFinal("CUSTOMER_KEY")===Account("CUSTOMER_KEY"),"Left").drop(Account("CUSTOMER_KEY"))
       .drop(Account("SUB_PRODUCT_CODE"))
       .drop(Account("PRODUCT_CODE"))*/
     //val x = finalTB.distinct()
    //x.toDF()
   }

}
