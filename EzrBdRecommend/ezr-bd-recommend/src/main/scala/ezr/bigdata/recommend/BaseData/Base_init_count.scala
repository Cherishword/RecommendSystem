package ezr.bigdata.recommend.BaseData

import ezr.bigdata.recommend.BaseData.Base_behavior_count.Data_insert_daytime
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, regexp_replace, when}

object Base_init_count {
  def main(args: Array[String]):Unit= {
    val shardingGrpid= args(0)
    val brandid = args(1)
    val daytime= args(2)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .appName("base_behavior_count pro"+shardingGrpid+" "+brandid)
      .config("spark.sql.hive.convertMetastoreOrc","true")
      .config("spark.sql.orc.impl","native")
      //      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    //    数据初始化  按行为日期分区插入表中
        Data_insert_init(spark,shardingGrpid,brandid,"mall_sales_order" ,"mall_sales_order_dtl" ,
          "rtl_prod","add_to_cart_dt_partition","browse_sku_page_dt_partition",
          "expose_sku_component_dt_partition","trigger_sku_component_dt_partition",daytime)
    //    第一次跑数据初始化过程
        ItemData_insert_init(spark,brandid,daytime)
    spark.stop()
  }
  def Data_insert_init(spark:SparkSession,shardingGrpid:String,brandid:String,mall_sales_order:String,mall_sales_order_dtl:String,rtl_prod:String,
                       add_to_cart_dt_partition:String,browse_sku_page_dt_partition:String,expose_sku_component_dt_partition:String,trigger_sku_component_dt_partition:String,daytime:String){
    //读取数据  把商品和用户点击行为做连接
    //  迄今为止前一百天的订单表和行为表数据
    val sql_order = "select id,buyerid user_id,DATE_FORMAT(createDate,'yyyy-MM-dd') behavior_time from pro."+ mall_sales_order+" where brandidpartition = "+brandid+" and dt = regexp_replace('"+daytime+"','-','') and createDate >= DATE_sub('"+daytime+"',100)"
    val sql_orders_dt1= "select orderid,itemid from pro."+mall_sales_order_dtl+" where brandidpartition = "+brandid+" and dt = regexp_replace('"+daytime+"','-','') and createDate >= DATE_sub('"+daytime+"',100)"
    val sql_prod ="select id,itemno from pro."+rtl_prod+" where brandidpartition="+brandid+" and dt = regexp_replace('"+daytime+"','-','') and itemno > ''"

    val sql_cart = "select vipid user_id,spu_id item_id,to_date(from_unixtime(cast(serverTime/1000 as int))) behavior_time,4 activetype from pro"+shardingGrpid+"."+add_to_cart_dt_partition+" where brandid = " + brandid +" and dt = regexp_replace('"+daytime+"','-','') and DATE_add(from_unixtime(cast(time/1000 as int)),100)>'"+daytime+"'"
    val sql_browse = "select vipid user_id,spu_id item_id,to_date(from_unixtime(cast(serverTime/1000 as int))) behavior_time,3 activetype  from pro"+shardingGrpid+"."+browse_sku_page_dt_partition+" where brandid = " + brandid +" and dt = regexp_replace('"+daytime+"','-','') and DATE_add(from_unixtime(cast(time/1000 as int)),100)>'"+daytime+"'"
    val sql_expose = "select vipid user_id,spu_id item_id,to_date(from_unixtime(cast(serverTime/1000 as int))) behavior_time,1 activetype  from pro"+shardingGrpid+"."+ expose_sku_component_dt_partition +" where brandid = " + brandid +" and dt = regexp_replace('"+daytime+"','-','') and DATE_add(from_unixtime(cast(time/1000 as int)),100)>'"+daytime+"'"
    val sql_trigger = "select vipid user_id,spu_id item_id,to_date(from_unixtime(cast(serverTime/1000 as int))) behavior_time,2 activetype  from pro"+shardingGrpid+"."+ trigger_sku_component_dt_partition +" where brandid = " + brandid +" and dt = regexp_replace('"+daytime+"','-','') and DATE_add(from_unixtime(cast(time/1000 as int)),100)>'"+daytime+"'"

    val order=spark.sql(sql_order)
    val orders_dt1=spark.sql(sql_orders_dt1)
    val prod = spark.sql(sql_prod)

    val order1 = order.join(orders_dt1,order("id")===orders_dt1("orderid"),"left")
    val buy = order1.join(prod,order1("itemid")=== prod("id"),"left").select("user_id","itemno","behavior_time")
      .withColumn("activetype",lit(5)).withColumnRenamed("itemno","item_id")
      .filter("item_id is not null")
    val behavior = buy.union(spark.sql(sql_cart)).union(spark.sql(sql_browse)).union(spark.sql(sql_expose)).union(spark.sql(sql_trigger)).withColumnRenamed("activetype","behavior")
      .withColumn("count",lit(1)).filter(col("user_id")>0)
    var base_behavior_statis =behavior.groupBy("user_id","item_id","behavior_time").pivot("behavior").sum("count").na.fill(0)
//    for (col<-bascollect_sete_behavior_statis.columns){
//      base_behavior_statis = base_behavior_statis.withColumn(col, when(base_behavior_statis(col).isNull, 0).otherwise(base_behavior_statis(col)))}
    base_behavior_statis=base_behavior_statis
      .withColumnRenamed("1","ExposeCount").withColumnRenamed("2","TriggerCount")
      .withColumnRenamed("3","BrowseCount").withColumnRenamed("4","CartCount")
      .withColumnRenamed("5","BuyCount").withColumn("brandid",lit(brandid))
      .withColumn("dt",regexp_replace(col("behavior_time"),"-",""))
    for (name <- Seq("ExposeCount","TriggerCount","BrowseCount","CartCount","BuyCount")){
      if (base_behavior_statis.columns.toList.contains(name)){}
      else{base_behavior_statis=base_behavior_statis.withColumn(name,lit(0))}}
    //写入hive
    base_behavior_statis.repartition(2).persist().createOrReplaceTempView("base_behavior_statis")


    val sql2 = "insert into table recommend_pro.base_behavior_count partition(brandid,dt) " +
      "select user_id,item_id,behavior_time,ExposeCount,TriggerCount,BrowseCount,CartCount,BuyCount,brandid,dt from base_behavior_statis"
    spark.sql(sql2)
  }
  def ItemData_insert_init(spark:SparkSession,brandid:String,daytime:String): Unit ={
    val sql_behavior ="select item_id,behavior_time,sum(ExposeCount) ExposeCount,sum(TriggerCount) TriggerCount,sum(BrowseCount) BrowseCount,sum(CartCount) CartCount,sum(BuyCount) BuyCount " +
      "from recommend_pro.base_behavior_count  where brandid = " + brandid +" and dt>regexp_replace(date_sub('"+daytime+"',100), '-', '') group by item_id,behavior_time"
    val behavior = spark.sql(sql_behavior)
    //读取数据  把商品和用户点击行为做连接
    val init_item_static = behavior.withColumn("dt",regexp_replace(col("behavior_time"),"-",""))
      .withColumn("brandid",lit(brandid))
    //写入hive
    init_item_static.repartition(2).persist().createOrReplaceTempView("init_item_static")
    val sql2 = "insert into table recommend_pro.base_item_count partition(brandid,dt) " +
      "select item_id,ExposeCount,TriggerCount,BrowseCount,CartCount,BuyCount,brandid,dt from init_item_static"
    spark.sql(sql2)

  }
}
