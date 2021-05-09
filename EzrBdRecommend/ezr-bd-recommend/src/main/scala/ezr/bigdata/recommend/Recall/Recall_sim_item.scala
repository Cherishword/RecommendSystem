package ezr.bigdata.recommend.Recall

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.ArrayBuffer
object Recall_sim_item {
  def main(args: Array[String]): Unit = {
//    val shardingid = args(0)
    val brandid: String = args(0)
    val dt =args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .appName("recall_tagsim_item pro " + brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val sql_prod_refresh = "select DATE_FORMAT(lastmodifieddate,'yyyyMMdd') from pro.rtl_prod prod where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition=" + brandid +" order by lastmodifieddate desc limit 1"
    val sql_category_refresh = "select DATE_FORMAT(lastmodifieddate,'yyyyMMdd') from pro.rtl_prod_category where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition=" + brandid +" order by lastmodifieddate desc limit 1"
    val dt_time1 =spark.sql(sql_prod_refresh).rdd.collect().toString
    val dt_time2 =spark.sql(sql_category_refresh).rdd.collect().toString
    if (dt_time1 >= dt.replace("-","") || dt_time2 >= dt.replace("-","")) {
    sim_item(spark,brandid)
   }
  }
def sim_item(spark:SparkSession,brandid:String): Unit ={
  import spark.implicits._
  //读取商品的所有标签
  val sql_item_tag = "select item_id item_idI,tag_type,tag from recommend_pro.feature_tag_item where brandid=" + brandid
  val item_tag = spark.sql(sql_item_tag)
  val item_cate1 = item_tag.where($"tag_type"===1)
  val item_cate2 = item_tag.where($"tag_type"===2)
  val item_cate3 = item_tag.where($"tag_type"===3)
  val item_price = item_tag.where($"tag_type"===4)
  val item_name = item_tag.where($"tag_type"===5)
  val item_sex = item_tag.where($"tag_type"===6)
  val item_season = item_tag.where($"tag_type"===7)

  //    聚合每个商品相关的标签
  val tag_collect1 = item_name.groupBy("item_idI").agg(collect_set("tag"))
    .withColumnRenamed("collect_set(tag)", "tag_setI")
    .join(item_cate1,Seq("item_idI"),"left").withColumnRenamed("tag","cate1I")
    .join(item_cate2,Seq("item_idI"),"left").withColumnRenamed("tag","cate2I")
    .join(item_cate3,Seq("item_idI"),"left").withColumnRenamed("tag","cate3I")
    .join(item_price,Seq("item_idI"),"left").withColumnRenamed("tag","price4I")
    .join(item_sex,Seq("item_idI"),"left").withColumnRenamed("tag","sex6I")
    .join(item_season,Seq("item_idI"),"left").withColumnRenamed("tag","season7I")
    .drop("tag_type").na.fill("nan")

  val tag_collect2 =tag_collect1.withColumnRenamed("item_idI","item_idJ")
    .withColumnRenamed("tag_setI", "tag_setJ")
    .withColumnRenamed("cate1I","cate1J")
    .withColumnRenamed("cate2I","cate2J")
    .withColumnRenamed("cate3I","cate3J")
    .withColumnRenamed("price4I","price4J")
    .withColumnRenamed("sex6I","sex6J")
    .withColumnRenamed("season7I","season7J")

  val tag_collect = tag_collect1.join(tag_collect2,
    tag_collect1("item_idI")=!=tag_collect2("item_idJ")
      &&tag_collect1("cate1I")===tag_collect2("cate1J")
      &&tag_collect1("cate2I")===tag_collect2("cate2J")
      &&tag_collect1("cate3I")===tag_collect2("cate3J")
      &&tag_collect1("sex6I")===tag_collect2("sex6J")
      &&tag_collect1("season7I")===tag_collect2("season7J")
    ,"full")

  val intersection_union = udf {
    (Set1: WrappedArray[String],Set2: WrappedArray[String]) =>
      (Set1.intersect(Set2)).size.toDouble/(Set1.union(Set2)).distinct.size.toDouble }
  val tag_sim = tag_collect.na.drop().withColumn("name_score", intersection_union($"tag_setI",$"tag_setJ"))
    .filter($"name_score">0.25)
    .withColumn("price_score",least($"price4I",$"price4J")/greatest($"price4I",$"price4J"))
     //.na.fill(0, Seq("price_score"))
    .select("item_idI","item_idJ","name_score","price_score")
  //数据归一化相加
  val tag_value = tag_sim
    .withColumn("name_score_scale",(($"name_score")-min("name_score").over())/(max("name_score").over()-min("name_score").over()))
    .withColumn("price_score_scale",(($"price_score")-min("price_score").over())/(max("price_score").over()-min("price_score").over()))
    .withColumn("score",lit(0.7)*$"name_score"+lit(0.3)*$"price_score")
  //排序选择前rank名商品   暂取排名前30
  val item_sim_rank =tag_value.withColumn("rank", row_number.over(Window.partitionBy("item_idI").orderBy(col("score").desc)))
    .where($"rank" <= 30)
  item_sim_rank.repartition(2).persist().createOrReplaceTempView("table")
  val sql1 = "insert overwrite table recommend_pro.recall_sim_item partition(brandid ="+brandid+") select Item_idI,Item_idJ,score from table"
  spark.sql(sql1)
  spark.stop()
}
}
