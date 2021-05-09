package ezr.bigdata.recommend.test

import org.apache.spark.sql.functions.{col, current_date, datediff, desc}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/2.
  *         商品的热度值计算
  */
object Recall_hot_result {
  def main(args: Array[String]): Unit = {
    val brandid: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Recall_hot_result pro"+brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //读取用户商品数据
    val sql_buy ="SELECT orders_dt1.BuyerId vipid, 4 activetype, prod.ItemNo, orders_dt1.daystime "+
      "FROM (SELECT orders.BrandId, orders.BuyerId, DATE_FORMAT(orders.CreateDate,'yyyy-MM-dd') daystime, dtl.ItemId "+
      "FROM tayouhaya_mall_tmp.mall_sales_order orders LEFT JOIN tayouhaya_mall_tmp.mall_sales_order_dtl dtl ON (orders.Id = dtl.OrderId AND orders.BrandId = dtl.BrandId) "+
      "WHERE orders.BrandId = "+ brandid+" and orders.CreateDate > DATE_sub(NOW(),100)) orders_dt1 "+
      "LEFT JOIN tayouhaya_mall_tmp.mall_prd_prod prod ON (orders_dt1.ItemId = prod.Id AND orders_dt1.BrandId = prod.BrandId) WHERE prod.ItemNo > ''"
    val sql_cart = "select vipid,1 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime from tayouhaya_youshu_tmp.add_to_cart where brandid = " + brandid +" and dt>20191107"
    val sql_click = "SELECT vipid,3 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime  FROM tayouhaya_youshu_tmp.browse_sku_page where brandid = " + brandid +" and dt>20191107 "
    val behavior = spark.sql(sql_buy).union(spark.sql(sql_cart)).union(spark.sql(sql_click))
    val hot = behavior.withColumnRenamed("itemno","item_id").withColumnRenamed("activetype","behavior")
    //按商品和时间排序及去重商品  得到商品最近行为时间
    val hot1 = hot.select( "item_id" , "daystime" ).sort(desc("item_id"),desc("daystime")).dropDuplicates("item_id")
    //计算商品几种行为的次数
    val hot2 = hot.stat.crosstab("item_id","behavior").withColumnRenamed("item_id_behavior", "item_id")
   // 商品最近行为的时间和行为频率作拼接
    val hot_num = hot1.join(hot2, "item_id").withColumn("daystime",col("daystime").cast(DateType)).withColumn("today",current_date())
    // 商品最近行为的时间迄今时间差
    val diff =  hot_num.withColumn("diff", datediff(col("today"), col("daystime")))
    //计算商品热度值
    val hot_value = diff.select($"item_id", ($"1"*0.1 + $"3"*0.2+ $"4"*0.4 - $"diff"*0.1).as("item_hot")).orderBy($"item_hot".desc).limit(100)
    //写入hive
    spark.sql("truncate table recommend_pro"+ brandid + ".recall_hot_result")
    hot_value.write.mode(SaveMode.Overwrite).insertInto("recommend_pro"+brandid + ".recall_hot_result")
    spark.stop()
  }
}
