package ezr.bigdata.recommend.Feature

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when

object Feature_tag_attr {
  def main(args: Array[String]): Unit = {
//    val shardingGrpid = args(0)
    val brandid: String = args(0)
    val dt =args(1)
    //环境
    val spark = SparkSession
      .builder()
      .appName("feature_tag_sex pro " + brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    //只要商品或商品的类目有变动，就重新跑一边数据
    val sql_prod_refresh = "select DATE_FORMAT(lastmodifieddate,'yyyyMMdd') from pro.rtl_prod prod where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition=" + brandid +" order by lastmodifieddate desc limit 1"
    val sql_category_refresh = "select DATE_FORMAT(lastmodifieddate,'yyyyMMdd') from pro.rtl_prod_category where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition=" + brandid +" order by lastmodifieddate desc limit 1"
    val dt_time1 =spark.sql(sql_prod_refresh).rdd.collect().toString
    val dt_time2 =spark.sql(sql_category_refresh).rdd.collect().toString
    if (dt_time1 >= dt.replace("-","") || dt_time2 >= dt.replace("-","")) {
      prod_attr(spark,brandid,dt)
    }
  }
  def prod_attr(spark:SparkSession,brandid:String,dt:String): Unit ={
    import spark.implicits._
    //读取商品属性json  取出商品的性别和季节属性
    val sql_prod_attr = "select itemno item_id,rr.AttrName ,tt.AttrVal tag from (select itemno,split(regexp_replace(regexp_replace(attrrelationsjson,'\\\\[|\\\\]',''),'\\\\}\\\\,\\\\{', '\\\\}\\\\|\\\\|\\\\{'),'\\\\|\\\\|') as attr from pro.rtl_prod where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition="+ brandid +") prod_attr " +
      "lateral view explode(prod_attr.attr) ss as col " +
      "lateral view json_tuple(ss.col,'AttrName','ProductAttrVals') rr as AttrName,ProductAttrVals " +
      "lateral view json_tuple(rr.ProductAttrVals,'AttrVal') tt as AttrVal"
    val prod_attr  = spark.sql(sql_prod_attr).filter($"AttrName"==="购买商品性别款"||$"AttrName"==="购买季节")
      .withColumn("tag_type",when($"AttrName" === "购买商品性别款",6).otherwise(7))
    prod_attr.repartition(2).persist().createOrReplaceTempView("table")
    val sql1 = "insert into table recommend_pro.feature_tag_item partition(brandid ="+brandid+") select item_id,tag_type,tag from table"
    spark.sql(sql1)
    spark.stop()
  }
}
