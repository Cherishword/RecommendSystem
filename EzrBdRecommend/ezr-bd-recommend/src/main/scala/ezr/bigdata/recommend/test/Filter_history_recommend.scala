package ezr.bigdata.recommend.test

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, row_number}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/19  过滤排序的商品
  */

object Filter_history_recommend {
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Filter_history_recommend pro"+brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //读取排序和用户行为数据
    val sql_r = "select user_id,item_id,item_name, categoryid, cuscategoryid, saleprice,click_prob,rank from recommend_pro"+brandid+".rank_ctr_result"
    val sql_buy ="SELECT orders_dt1.BuyerId vipid, 4 activetype, prod.ItemNo, orders_dt1.daystime "+
      "FROM (SELECT orders.BrandId, orders.BuyerId, DATE_FORMAT(orders.CreateDate,'yyyy-MM-dd') daystime, dtl.ItemId "+
      "FROM tayouhaya_mall_tmp.mall_sales_order orders LEFT JOIN tayouhaya_mall_tmp.mall_sales_order_dtl dtl ON (orders.Id = dtl.OrderId AND orders.BrandId = dtl.BrandId) "+
      "WHERE orders.BrandId = "+ brandid+" and orders.CreateDate > DATE_sub(NOW(),100)) orders_dt1 "+
      "LEFT JOIN tayouhaya_mall_tmp.mall_prd_prod prod ON (orders_dt1.ItemId = prod.Id AND orders_dt1.BrandId = prod.BrandId) WHERE prod.ItemNo > ''"
    val sql_cart = "select vipid,1 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime from tayouhaya_youshu_tmp.add_to_cart where brandid = " + brandid +" and dt>20191107"
    val sql_click = "SELECT vipid,3 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime  FROM tayouhaya_youshu_tmp.browse_sku_page where brandid = " + brandid +" and dt>20191107 "
    //    val sql_u = "select vipid as user_id,itemno as item_id from recommend_pro"+brandid+".user_item_behavior"
    val click_items = spark.sql(sql_buy).union(spark.sql(sql_cart)).union(spark.sql(sql_click)).select("vipid","itemno").withColumnRenamed("vipid","user_id")
      .withColumnRenamed("itemno","item_id").dropDuplicates("user_id","item_id")
    val rank = spark.sql(sql_r)
    //用户和商品字段组合
    val click_items_ws = click_items.select(concat_ws("",$"user_id", $"item_id").as("ui"))
    val rank_ws = rank.withColumn("ui",concat_ws("",$"user_id", $"item_id"))
    //过滤掉用户已经有行为的商品
    val rank_not_isin = rank_ws.filter(!col("ui").isin(click_items_ws.select("ui").collect().map(_(0)).toList:_*)).drop("ui")
    //以商品目录排序   获得用户点击类目的概率的排名
    val rank_cate = rank_not_isin.withColumn("cate_rank",row_number.over(Window.partitionBy("user_id","categoryid", "cuscategoryid")
                           .orderBy(col("click_prob").desc))).drop("rank")
    //形成类目交叉   每个类目的的排序
    val  rank_filter_cate = rank_cate.withColumn("rank",row_number.over(Window.partitionBy("user_id")
                          .orderBy(col("cate_rank").asc))).select("user_id","item_id","item_name", "saleprice","click_prob","rank","cate_rank")
    //数据导入数据库
    spark.sql("truncate table recommend_pro"+brandid+".filter_history_recommend")
    rank_filter_cate.write.mode(SaveMode.Overwrite).insertInto("recommend_pro"+brandid+".filter_history_recommend")
    spark.stop()
  }
}
