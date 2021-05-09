package ezr.bigdata.recommend.Save

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import ezp.bigdata.database.entity.ConfigHelper
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.{Put, Result}

object Rec_Save_Hbase {
  def main(args: Array[String]): Unit = {
    val brandid: String = args(0)
    val daytime = args(1)

    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SavetoHbase pro " + brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val sql_browse = " select concat_ws('',brandid,item_id) brand_item,similar_browse_item item_idJ,similar_value from recommend_pro.feature_similar_browse where brandid = " + brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
    val sql_cart = " select concat_ws('',brandid,item_id) brand_item,similar_cart_item item_idJ,similar_value from recommend_pro.feature_similar_cart where brandid = " + brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
    val sql_buy = " select concat_ws('',brandid,item_id) brand_item,similar_buy_item item_idJ,similar_value from recommend_pro.feature_similar_buy where brandid = " + brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
    val sql_name =  " select concat_ws('',"+brandid+",item_idI) brand_item,item_idJ,similar_value from recommend_pro.recall_sim_item where brandid = " + brandid
//    浏览 加购 购买 商品名归一化
    val browse = spark.sql(sql_browse).withColumn("scale_value",(($"similar_value")-min("similar_value").over())/(max("similar_value").over()-min("similar_value").over()))
    val cart = spark.sql(sql_cart).withColumn("scale_value",(($"similar_value")-min("similar_value").over())/(max("similar_value").over()-min("similar_value").over()))
    val buy = spark.sql(sql_buy).withColumn("scale_value",(($"similar_value")-min("similar_value").over())/(max("similar_value").over()-min("similar_value").over()))
    val name_sim = spark.sql(sql_name).withColumn("scale_value",(($"similar_value")-min("similar_value").over())/(max("similar_value").over()-min("similar_value").over()))
    val item_sim = browse.union(cart).union(buy).union(name_sim).groupBy("brand_item","item_idJ").agg(sum("scale_value"))
      .withColumn("scale_rank", row_number.over(Window.partitionBy("brand_item").orderBy(col("sum(scale_value)").desc))).filter($"scale_rank"<=30)
    .select("brand_item","scale_rank","item_idJ")
//    item_sim.rdd.foreachPartition{ rdd=>
//      val conf = HBaseConfiguration.create()
//      conf.set("hbase.zookeeper.quorum", "172.21.17.226")
//      conf.set("hbase.zookeeper.property.clientPort", "2181")
//      val connection = ConnectionFactory.createConnection(conf)
////      val admin = connection.getAdmin
//      val myTable = connection.getTable(TableName.valueOf("item_sim"))
//      rdd.foreach{row=>
//      val rowkey= row(0).toString
//      val columnFamily= "cf"
//      val column = row(1).toString
//      val value =  row(2).toString
//      val put = new Put(Bytes.toBytes(rowkey))
//      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
//        myTable.put(put)

    val context = spark.sparkContext
    context.hadoopConfiguration.set("hbase.zookeeper.quorum", ConfigHelper.hbaseZookeeperQuorum)
    context.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    context.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "item_sim")

    val job = Job.getInstance(context.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val rdd = item_sim.rdd.map(row => {
//      val brandId = date.getAs[Int]("brandId")
//      val openId = date.getAs[String]("openId")
//      val put = new Put(Bytes.toBytes(createRowKey(brandId, openId)))
//      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(dt.toString))
      val rowkey= row(0).toString
      val columnFamily= "cf"
      val column = row(1).toString
      val value =  row(2).toString
      val put = new Put(Bytes.toBytes(rowkey))

      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
      println()
      (new ImmutableBytesWritable, put)
    })

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
  def createRowKey(branId: Int, openId: String): String = {
    //     new StringBuffer(vipId.formatted("%011d")).reverse().toString + branId.formatted("%05d")
    openId + branId.formatted("%04d")
  }
}

