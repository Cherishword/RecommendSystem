package ezr.bigdata.recommend.Recall

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

/**
  * @author wuyuantin@easyretailpro.com
  *         2020/05/27
  *         会员对商品的偏好表  以三个月内行为次数（浏览 加购 购买）作为对商品喜好程度
  */

object Recall_cf_item {
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    val daytime= args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Recall_cf_item pro "+brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //读取用户购买行为数据
    val sql_behavior = "select user_id,item_id,BrowseCount,CartCount,BuyCount,behavior_time from recommend_pro.base_behavior_count where brandid = " + brandid +" and dt>regexp_replace(date_sub('"+daytime+"',100), '-', '')"
    val behavior = spark.sql(sql_behavior)
    // 商品行为次数/商品行为迄今时间差作为该用户对商品偏好程度    然后合计汇总
    val behavior_browse_sum = behavior.select("user_id","item_id","BrowseCount","behavior_time").where(col("BrowseCount")>0)
      .withColumn("prefer", col("BrowseCount")/datediff(lit(daytime), col("behavior_time")))
     .groupBy("user_id","item_id").sum("prefer").withColumnRenamed("sum(prefer)","prefer")
    val behavior_cart_sum = behavior.select("user_id","item_id","CartCount","behavior_time").where(col("CartCount")>0)
      .withColumn("prefer", col("CartCount")/datediff(lit(daytime), col("behavior_time")))
      .groupBy("user_id","item_id").sum("prefer").withColumnRenamed("sum(prefer)","prefer")
    val behavior_buy_sum = behavior.select("user_id","item_id","BuyCount","behavior_time").where(col("BuyCount")>0)
     .withColumn("prefer", col("BuyCount")/datediff(lit(daytime), col("behavior_time")))
     .groupBy("user_id","item_id").sum("prefer").withColumnRenamed("sum(prefer)","prefer")
//依据用户各行为（协同过滤）获得相似商品
    val behavior_browse_similar = similarity(spark,behavior_browse_sum,30)
    val behavior_cart_similar = similarity(spark,behavior_cart_sum,30)
    val behavior_buy_similar = similarity(spark,behavior_buy_sum,30)
   // 商品相似数据插入hive
    InsertHiveTable(spark,brandid,daytime,behavior_browse_similar,"feature_similar_browse")
    InsertHiveTable(spark,brandid,daytime,behavior_cart_similar,"feature_similar_cart")
    InsertHiveTable(spark,brandid,daytime,behavior_buy_similar,"feature_similar_buy")
    //依据用户行为取行为相似商品排名前30商品
    val behavior_browse_rank = user_prefer(spark,behavior_browse_similar,behavior_browse_sum,30)
    val behavior_cart_rank = user_prefer(spark,behavior_cart_similar,behavior_cart_sum,30)
    val behavior_buy_rank = user_prefer(spark,behavior_buy_similar,behavior_buy_sum,30)
   //写入hive
    InsertHiveTable(spark,brandid,daytime,behavior_browse_rank,"recall_cf_browse")
    InsertHiveTable(spark,brandid,daytime,behavior_cart_rank,"recall_cf_cart")
    InsertHiveTable(spark,brandid,daytime,behavior_buy_rank,"recall_cf_buy")
    spark.stop()
  }
def similarity(spark:SparkSession,user_ds:DataFrame,rank:Int): DataFrame ={
  import spark.implicits._
  // 用户分组，同一个用户购买的商品放一起   (用户：物品) => (用户：(物品集合))
  val user_ds1: DataFrame =user_ds.groupBy("user_id").agg(collect_set("item_id"))
    .withColumnRenamed("collect_set(item_id)", "item_id_set")
  //    物品:物品
    val user_ds2 = user_ds1.flatMap { row =>
    val itemlist = row.getAs[scala.collection.mutable.WrappedArray[String]](1).toArray
    val result = new ArrayBuffer[(String, String, String)]()
    for (i <- 0 to itemlist.length - 2) {
      for (j <- i + 1 to itemlist.length - 1) {
        result += ((itemlist(i), itemlist(j), "1"))
      }}
    result
  }.withColumnRenamed("_1", "itemidI").withColumnRenamed("_2", "itemidJ")
    .withColumnRenamed("_3", "score")
  //  物品分组  计算物品与物品 同现频次(同时被购买次数)
  val user_ds3 = user_ds2.groupBy("itemidI", "itemidJ").agg(sum("score").as("sumIJ"))
  //  计算物品总共出现的频次
  val user_ds0 = user_ds.withColumn("score", lit(1)).groupBy("item_id").agg(sum("score").as("score"))
  // 计算同现相似度
      val user_ds4 = user_ds3.join(user_ds0.withColumnRenamed("item_id", "itemidJ").withColumnRenamed("score", "sumJ").select("itemidJ", "sumJ"), "itemidJ")
      val user_ds5 = user_ds4.join(user_ds0.withColumnRenamed("item_id", "itemidI").withColumnRenamed("score", "sumI").select("itemidI", "sumI"), "itemidI")
      // 根据公式N(i)∩N(j)/sqrt(N(i)*N(j)) 计算
      val user_ds6 = user_ds5.withColumn("result", col("sumIJ") / sqrt(col("sumI") * col("sumJ")))
      //合并
      val user_ds7 = user_ds6.select("itemidI", "itemidJ", "result").union(user_ds6.select($"itemidJ".as("itemidI"), $"itemidI".as("itemidJ"), $"result"))

      val user_ds8 = user_ds7.withColumn("rank", row_number.over(Window.partitionBy("itemidI").orderBy(col("result").desc)))
        .where($"rank" <= rank).drop("rank").withColumnRenamed("result","similar")
       user_ds8
       }
  def user_prefer(spark:SparkSession,items_similar:DataFrame,user_prefer:DataFrame,rank:Int): DataFrame ={
     import spark.implicits._
    //依据用户行为item召回相似的item
     val user_prefer_ds1 = items_similar.join(user_prefer,$"itemidI" === $"item_id","inner")
   //计算用户召回相似商品的得分
    val user_prefer_ds2 = user_prefer_ds1.withColumn("score",col("similar")*col("prefer")*100)
      .select("user_id","itemidJ","score")
    //汇总每个用户对相似商品偏好分数
    val user_prefer_ds3 = user_prefer_ds2.groupBy("user_id","itemidJ").agg(sum("score").as("score"))
      .withColumnRenamed("itemidJ","item_id")//.drop("")
    //过滤用户已有行为商品
    val user_prefer_ds4 = user_prefer_ds3.join(user_prefer,Seq("user_id","item_id"),"left")
      .where("prefer is null")
    //排序选择前rank名商品   暂取排名前30
    val user_prefer_ds5 =user_prefer_ds4.withColumn("rank", row_number.over(Window.partitionBy("user_id").orderBy(col("score").desc)))
        .where($"rank" <= rank)
    user_prefer_ds5.select("user_id","item_id","score")
  }
  def InsertHiveTable(spark:SparkSession,brandid:String,daytime:String,Temptable:DataFrame,hive_table:String): Unit ={
        Temptable.repartition(2).persist().createOrReplaceTempView("table")
        val sql1 =  "alter table recommend_pro."+ hive_table+" drop IF EXISTS partition(brandid = "+brandid+",dt="+daytime.replace("-","")+")"
        val sql2 = "insert into table recommend_pro."+ hive_table+" partition(brandid = "+brandid+",dt="+daytime.replace("-","")+") select * from table"
        spark.sql(sql1)
        spark.sql(sql2)


    val beforeHundredDay = new DateTime(daytime).minusDays(3).toString("yyyMMdd")
    spark.sql(s"alter table recommend_pro.$hive_table drop if exists partition(brandid = $brandid,dt=$beforeHundredDay)")


  }
}