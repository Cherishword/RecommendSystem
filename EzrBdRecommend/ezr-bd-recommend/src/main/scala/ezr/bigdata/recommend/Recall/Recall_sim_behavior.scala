package ezr.bigdata.recommend.Recall

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object Recall_sim_behavior {
  def main(args: Array[String]): Unit = {
     val brandid: String = args(0)
    val daytime = args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Recall_sim_behavior " + brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //   选取召回的商品合并
    //读取用户购买行为数据
    val sql_behavior = "select user_id,item_id,TriggerCount,BrowseCount,CartCount,BuyCount,behavior_time from recommend_pro.base_behavior_count where brandid = " + brandid + " and dt>regexp_replace(date_sub('" + daytime + "',100), '-', '')"
    val sql_sim_item = "select item_idI,item_idJ,similar_value from recommend_pro.recall_sim_item where brandid = " + brandid
    val behavior = spark.sql(sql_behavior).withColumn("diff",datediff(lit(daytime), col("behavior_time")))
    val sim_item = spark.sql(sql_sim_item).withColumnRenamed("item_idI","item_id")
//    过滤没有行为的商品
    val filter = behavior.where($"TriggerCount"+$"BrowseCount"+$"CartCount"+$"BuyCount">0)
    //用户点击的商品和相似商品做关联，推荐相似的商品给用户
    val user_sim_filter = filter.join(sim_item,Seq("item_id"),"left").select("user_id","item_idJ","similar_value","diff")
      .groupBy("user_id", "Item_idJ").agg(sum($"similar_value"/$"diff").as("score")).withColumnRenamed("item_idJ","item_id")
      .where("item_id is not null")
    //过滤用户已有行为商品
//    val user_sim_filter = user_sim.join(filter,Seq("user_id","item_id"),"left").where("diff is null")
    val user_sim_rank = user_sim_filter.withColumn("rank", row_number.over(Window.partitionBy("user_id").orderBy(col("score").desc)))
      .where($"rank" <= 30)

    user_sim_rank.repartition(2).persist().createOrReplaceTempView("table")
    val sql1 =  "alter table recommend_pro.recall_sim_behavior drop IF EXISTS partition(brandid ="+brandid+",dt="+daytime.replace("-","")+")"
    val sql2 = "insert into table recommend_pro.recall_sim_behavior partition(brandid ="+brandid+",dt="+daytime.replace("-","")+") select user_id,item_id,score from table"
    spark.sql(sql1)
    spark.sql(sql2)

    val beforeHundredDay = new DateTime(daytime).minusDays(3).toString("yyyMMdd")
    spark.sql(s"alter table recommend_pro.recall_sim_behavior drop if exists partition(brandid = $brandid,dt=$beforeHundredDay)")

  }
}
