package ezr.bigdata.recommend.Recall

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.StringType
import org.joda.time.DateTime
/**
  * @author wuyuantin@easyretailpro.com
  *         2020/05/27
  *         商品的热度值计算  以行为次数计算综合热度值
  */
object Recall_hot_item {
  def main(args: Array[String]): Unit = {
    val brandid: String = args(0)
    val daytime: String = args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Recall_hot_item pro "+brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //读取用户商品数据
    val sql_item_behavior="SELECT item_id,dt,BrowseCount,CartCount,BuyCount from recommend_pro.base_item_count  where brandid = " + brandid+" and dt>=regexp_replace(date_sub('"+daytime+"',100), '-', '')"
    val item_behavior = spark.sql(sql_item_behavior)
    //    每个商品最近的行为时间差
    val behavior = item_behavior.withColumn("behavior_day_diff",datediff(lit(daytime),from_unixtime(unix_timestamp(col("dt").cast(StringType),"yyyyMMdd"),"yyyy-MM-dd")))
      .select("item_id","behavior_day_diff","BrowseCount","CartCount","BuyCount")

    // 商品三个月的曝光 购买 加购 浏览行为次数
    val item_total = behavior.groupBy("item_id").agg(sum("BrowseCount").as("Total_BrowseCount"),
      sum("CartCount").as("Total_CartCount"),sum("BuyCount").as("Total_BuyCount"),
      min("behavior_day_diff").as("diff"))
      .withColumn("Total_Ctr",lit("")).withColumn("Total_Cvr",lit(""))
      .withColumn("Total_HotValue",lit(0.1)*col("Total_BrowseCount")+lit(0.2)*col("Total_CartCount")+ lit(0.4)*col("Total_BuyCount")-lit(0.1)*col("diff"))

    //    商品两个月的曝光 购买 加购 浏览行为次数
    val item_TwoMonth = behavior.where(col("behavior_day_diff")<=lit(60))
      .groupBy("item_id").agg(sum("BrowseCount").as("TwoMonth_BrowseCount"),
      sum("CartCount").as("TwoMonth_CartCount"),sum("BuyCount").as("TwoMonth_BuyCount"),
      min("behavior_day_diff").as("diff"))
      .withColumn("TwoMonth_Ctr",lit("")).withColumn("TwoMonth_Cvr",lit(""))
      .withColumn("TwoMonth_HotValue",lit(0.1)*col("TwoMonth_BrowseCount")+lit(0.2)*col("TwoMonth_CartCount")+ lit(0.4)*col("TwoMonth_BuyCount")-lit(0.1)*col("diff"))

    //    商品一个月的曝光 购买 加购 浏览行为次数
    val item_OneMonth = behavior.where(col("behavior_day_diff")<=lit(30))
      .groupBy("item_id").agg(sum("BrowseCount").as("OneMonth_BrowseCount"),
      sum("CartCount").as("OneMonth_CartCount"),sum("BuyCount").as("OneMonth_BuyCount"),
      min("behavior_day_diff").as("diff"))
      .withColumn("OneMonth_Ctr",lit("")).withColumn("OneMonth_Cvr",lit(""))
      .withColumn("OneMonth_HotValue",lit(0.1)*col("OneMonth_BrowseCount")+lit(0.2)*col("OneMonth_CartCount")+ lit(0.4)*col("OneMonth_BuyCount")-lit(0.1)*col("diff"))

    // 商品一个星期的曝光 购买 加购 浏览行为
    val item_OneWeek = behavior.where(col("behavior_day_diff")<=lit(7))
      .groupBy("item_id").agg(sum("BrowseCount").as("OneWeek_BrowseCount"),
      sum("CartCount").as("OneWeek_CartCount"),sum("BuyCount").as("OneWeek_BuyCount"),
      min("behavior_day_diff").as("diff"))
      .withColumn("OneWeek_Ctr",lit("")).withColumn("OneWeek_Cvr",lit(""))
      .withColumn("OneWeek_HotValue",lit(0.1)*col("OneWeek_BrowseCount")+lit(0.2)*col("OneWeek_CartCount")+ lit(0.4)*col("OneWeek_BuyCount")-lit(0.1)*col("diff"))

    //    商品一天的曝光 购买 加购 浏览行为次数
    val item_OneDay = behavior.where(col("behavior_day_diff")<=lit(1))
      .groupBy("item_id").agg(sum("BrowseCount").as("OneDay_BrowseCount"),
      sum("CartCount").as("OneDay_CartCount"),sum("BuyCount").as("OneDay_BuyCount"),
      min("behavior_day_diff").as("diff"))
      .withColumn("OneDay_Ctr",lit("")).withColumn("OneDay_Cvr",lit(""))
      .withColumn("OneDay_HotValue",lit(0.1)*col("OneDay_BrowseCount")+lit(0.2)*col("OneDay_CartCount")+ lit(0.4)*col("OneDay_BuyCount")-lit(0.1)*col("diff"))

    // 商品最近行为的商品行为次数拼接
    val hot_value = item_total.join(item_TwoMonth,Seq("item_id"),"left")
      .join(item_OneMonth,Seq("item_id"),"left").join(item_OneWeek,Seq("item_id"),"left")
      .join(item_OneDay,Seq("item_id"),"left")
//    spark.sql("truncate table recommend_pro"+ brandid + ".recall_hot_result")
//    hot_value.write.mode(SaveMode.Overwrite).insertInto("recommend_pro"+brandid + ".recall_hot_result")
    val sql1 =  "alter table recommend_pro.recall_hot_item drop IF EXISTS partition(brandid ="+brandid+",dt="+daytime.replace("-","")+")"
    hot_value.repartition(2).persist().createOrReplaceTempView("hot_value")
    val sql2 = "insert into table recommend_pro.recall_hot_item partition(brandid ="+brandid+",dt="+daytime.replace("-","")+")" +
      " select item_id,Total_BrowseCount,Total_CartCount,Total_BuyCount,Total_Ctr,Total_Cvr,Total_HotValue," +
      "TwoMonth_BrowseCount,TwoMonth_CartCount,TwoMonth_BuyCount,TwoMonth_Ctr,TwoMonth_Cvr,TwoMonth_HotValue," +
      "OneMonth_BrowseCount,OneMonth_CartCount,OneMonth_BuyCount,OneMonth_Ctr,OneMonth_Cvr,OneMonth_HotValue," +
      "OneWeek_BrowseCount,OneWeek_CartCount,OneWeek_BuyCount,OneWeek_Ctr,OneWeek_Cvr,OneWeek_HotValue," +
      "OneDay_BrowseCount,OneDay_CartCount,OneDay_BuyCount,OneDay_Ctr,OneDay_Cvr,OneDay_HotValue from hot_value"
    spark.sql(sql1)
    spark.sql(sql2)

    val beforeHundredDay = new DateTime(daytime).minusDays(3).toString("yyyMMdd")
    spark.sql(s"alter table recommend_pro.recall_hot_item drop if exists partition(brandid = $brandid,dt=$beforeHundredDay)")

    spark.stop()
  }
}
