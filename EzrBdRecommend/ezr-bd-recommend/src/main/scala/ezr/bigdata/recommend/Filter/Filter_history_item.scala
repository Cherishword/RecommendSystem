package ezr.bigdata.recommend.Filter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

import scala.collection.mutable.WrappedArray
/**
  * @author wuyuantin@easyretailpro.com
  *         2020/06/04  过滤排序的商品（百日内存在浏览 加购 购买的行为）
  */
object Filter_history_item {
  def main(args: Array[String]): Unit = {
    val brandid: String = args(0)
    val daytime = args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Filter_history_item pro " + brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //读取用户购买行为数据
    val sql_behavior = "select user_id,item_id,BrowseCount,CartCount,BuyCount from recommend_pro.base_behavior_count where brandid = " + brandid +" and dt>regexp_replace(date_sub('"+daytime+"',100), '-', '')"
    val sql_rank = "select user_id,item_id,click_prob,rank from recommend_pro.rank_ctr_item where brandid = " + brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
    val sql_attr = "select item_id,tag from recommend_pro.feature_tag_item where tag_type = 6 and brandid = " + brandid
    val behavior = spark.sql(sql_behavior)
    val rank =spark.sql(sql_rank)
    val prod_attr = spark.sql(sql_attr)
    val user_behavior = behavior.join(prod_attr,Seq("item_id"),"left").filter($"BrowseCount"+$"CartCount"+$"BuyCount">0)
    val user_attr = user_behavior.groupBy("user_id").agg(collect_set("tag"))
    val  filter = user_behavior.select("user_id","item_id","BrowseCount")
//    筛选不同类的商品属性
    val udf_contains = udf {(Set1: WrappedArray[String],Set2: String) =>if (Set1.mkString(",").contains(Set2)) "1" else "0"}
    //过滤用户已有行为商品
    val user_sim_filter = rank.join(filter,Seq("user_id","item_id"),"left").where("BrowseCount is null")
         .select("user_id","item_id","click_prob")
      //      添加商品的性别属性
          .join(prod_attr,Seq("item_id"),"left")
//      添加用户行为的性别属性
        .join(user_attr,Seq("user_id"),"left")
//      null表示没有行为  array空表示用户有行为的商品不在库存内
//        .filter($"collect_set(tag)".cast(StringType).isNotNull && $"collect_set(tag)".cast(StringType) =!="[]"&& $"collect_set(tag)".cast(StringType) =!="[中性]")
//      .filter("tag is not null").
//      withColumn("filter",udf_contains($"collect_set(tag)",$"tag"))
      val user_filter_attr = user_sim_filter//.filter($"filter"=== "1")

    val filter_rank = user_filter_attr.withColumn("rank", row_number.over(Window.partitionBy("user_id")
      .orderBy(col("click_prob").desc)))
//
    filter_rank.repartition(2).persist().createOrReplaceTempView("table")
    val sql1 =  "alter table recommend_pro.filter_history_item drop IF EXISTS partition(brandid = "+brandid+",dt="+daytime.replace("-","")+")"
    val sql2 = "insert into table recommend_pro.filter_history_item partition(brandid = "+brandid+",dt="+daytime.replace("-","")+") select user_id,item_id,click_prob,rank from table"
    spark.sql(sql1)
    spark.sql(sql2)

    val beforeHundredDay = new DateTime(daytime).minusDays(3).toString("yyyMMdd")
    spark.sql(s"alter table recommend_pro.filter_history_item drop if exists partition(brandid = $brandid,dt=$beforeHundredDay)")

    spark.stop()
  }
}