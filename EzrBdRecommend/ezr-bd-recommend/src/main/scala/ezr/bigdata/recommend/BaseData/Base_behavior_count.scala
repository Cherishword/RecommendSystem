package ezr.bigdata.recommend.BaseData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

/**
  * @author wuyuantin@easyretailpro.com
  *         2020/05/27
  *         会员对商品的行为表，具体到天
  */
object Base_behavior_count {
  def main(args: Array[String]):Unit= {
    val shardingGrpid= args(0)
    val brandid = args(1)
    val daytime= args(2)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("base_behavior_count pro"+shardingGrpid+" "+brandid)
      .config("spark.sql.hive.convertMetastoreOrc","true")
      .config("spark.sql.orc.impl","native")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
//每日把昨天数据写入hive表
        Data_insert_daytime(spark,shardingGrpid,brandid,"mall_sales_order" ,"mall_sales_order_dtl" ,"rtl_prod","add_to_cart_dt_partition",
            "browse_sku_page_dt_partition","expose_sku_component_dt_partition","trigger_sku_component_dt_partition",daytime.replace("-",""))
    spark.stop()
}
  def Data_insert_daytime(spark:SparkSession,shardingGrp:String,brandid:String,mall_sales_order:String,mall_sales_order_dtl:String,rtl_prod:String,
                       add_to_cart_dt_partition:String,browse_sku_page_dt_partition:String,expose_sku_component_dt_partition:String,trigger_sku_component_dt_partition:String,daytime:String){
    //读取数据  把商品和用户点击行为做连接
//    订单表
val sql_order = "select id,buyerid user_id,DATE_FORMAT(createDate,'yyyy-MM-dd') behavior_time from pro."+ mall_sales_order+" where brandidpartition = "+brandid+" and dt = "+daytime+" and DATE_FORMAT(createDate,'yyyyMMdd') = "+daytime
    //    订单详情表
    val sql_orders_dt1= "select orderid,itemid from pro."+mall_sales_order_dtl+" where brandidpartition = "+brandid+"  and dt = "+daytime+" and DATE_FORMAT(createDate,'yyyyMMdd') = "+daytime
    //    订单对应的商品
    val sql_prod ="select id,itemno from pro."+rtl_prod+" where brandidpartition="+brandid+" and dt = "+daytime+" and itemno > ''"
    //    会员加购数据
    val sql_cart = "select vipid user_id,spu_id item_id,to_date(from_unixtime(cast(serverTime/1000 as int))) behavior_time,4 activetype from pro"+shardingGrp+"."+add_to_cart_dt_partition+" where brandid = " + brandid +" and dt = "+daytime+" and regexp_replace(to_date(from_unixtime(cast(time/1000 as int))),'-', '')="+daytime
    //    会员浏览数据
    val sql_browse = "select vipid user_id,spu_id item_id,to_date(from_unixtime(cast(serverTime/1000 as int))) behavior_time,3 activetype  from pro"+shardingGrp+"."+browse_sku_page_dt_partition+" where brandid = " + brandid +" and dt = "+daytime+" and regexp_replace(to_date(from_unixtime(cast(time/1000 as int))),'-', '')="+daytime
    //    会员曝光数据
    val sql_expose = "select vipid user_id,spu_id item_id,to_date(from_unixtime(cast(serverTime/1000 as int))) behavior_time,1 activetype  from pro"+shardingGrp+"."+ expose_sku_component_dt_partition +" where brandid = " + brandid +" and dt = "+daytime+" and regexp_replace(to_date(from_unixtime(cast(time/1000 as int))),'-', '')="+daytime
    //    会员触发数据
    val sql_trigger = "select vipid user_id,spu_id item_id,to_date(from_unixtime(cast(serverTime/1000 as int))) behavior_time,2 activetype  from pro"+shardingGrp+"."+ trigger_sku_component_dt_partition +" where brandid = " + brandid +" and dt = "+daytime+" and regexp_replace(to_date(from_unixtime(cast(time/1000 as int))),'-', '')="+daytime
    val order=spark.sql(sql_order)
    val orders_dt1=spark.sql(sql_orders_dt1)
    val prod = spark.sql(sql_prod)
//    订单表、订单详情表、商品表相互做关联
    val order1 = order.join(orders_dt1,order("id")===orders_dt1("orderid"),"left")
    val buy = order1.join(prod,order1("itemid")=== prod("id"),"left").select("user_id","itemno","behavior_time")
      .withColumn("activetype",lit(5)).withColumnRenamed("itemno","item_id")
      .filter("item_id is not null")
//    五种行为 浏览、加购、购买、曝光、触发关联一起
    val behavior = buy.union(spark.sql(sql_cart)).union(spark.sql(sql_browse)).union(spark.sql(sql_expose)).union(spark.sql(sql_trigger)).withColumnRenamed("activetype","behavior")
      .withColumn("count",lit(1)).filter(col("user_id")>0)
//以会员 商品 日期做分组，汇总时间下五种行为次数
    var base_behavior_statis =behavior.groupBy("user_id","item_id","behavior_time").pivot("behavior").sum("count").na.fill(0)
//    for (col<-base_behavior_statis.columns){
//      base_behavior_statis = base_behavior_statis.withColumn(col, when(base_behavior_statis(col).isNull, 0).otherwise(base_behavior_statis(col)))}
    base_behavior_statis=base_behavior_statis
     .withColumnRenamed("1","ExposeCount").withColumnRenamed("2","TriggerCount")
     .withColumnRenamed("3","BrowseCount").withColumnRenamed("4","CartCount")
     .withColumnRenamed("5","BuyCount")
//      .withColumn("dt",regexp_replace(col("behavior_time"),"-",""))
//判断会员是否存在浏览 加购 购买 曝光 触发数据   没有的添加列名
    for (name <- Seq("ExposeCount","TriggerCount","BrowseCount","CartCount","BuyCount")){
      if (base_behavior_statis.columns.toList.contains(name)){}
      else{base_behavior_statis=base_behavior_statis.withColumn(name,lit(0))}}
    //写入hive
//      数据注册临时表
    base_behavior_statis.repartition(2).persist().createOrReplaceTempView("base_behavior_statis")
//      如果行为数据分区存在即删除
    val sql1 =  "alter table recommend_pro.base_behavior_count drop IF EXISTS partition(brandid = "+brandid+",dt="+daytime+")"
//    数据插入分区
    val sql2 = "insert into table recommend_pro.base_behavior_count partition(brandid = "+brandid+",dt="+daytime+") select user_id,item_id,behavior_time,ExposeCount,TriggerCount,BrowseCount,CartCount,BuyCount from base_behavior_statis"
      spark.sql(sql1)
    spark.sql(sql2)

    val beforeHundredDay = new DateTime(daytime.substring(0,4)+"-"+daytime.substring(5,6)+"-"+daytime.substring(7,8)).minusDays(100).toString("yyyMMdd")
    spark.sql(s"alter table recommend_pro.base_behavior_count drop if exists partition(brandid = $brandid,dt=$beforeHundredDay)")

  }
}

