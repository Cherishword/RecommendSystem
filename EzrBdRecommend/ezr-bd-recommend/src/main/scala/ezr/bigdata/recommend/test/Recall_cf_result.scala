package ezr.bigdata.recommend.test

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/15.
  *         商品的协同过滤   w(i,j) = N(i)∩N(j)/sqrt(N(i)*N(j))
  */

object Recall_cf_result {
//  case class ItemSimi(itemidI: String, itemidJ: String,similar:Double)
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Recall_cf_result pro"+brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //读取用户购买行为数据和商品数据
    val sql_buy ="SELECT orders_dt1.BuyerId vipid, 4 activetype, prod.ItemNo, orders_dt1.daystime "+
      "FROM (SELECT orders.BrandId, orders.BuyerId, DATE_FORMAT(orders.CreateDate,'yyyy-MM-dd') daystime, dtl.ItemId "+
      "FROM tayouhaya_mall_tmp.mall_sales_order orders LEFT JOIN tayouhaya_mall_tmp.mall_sales_order_dtl dtl ON (orders.Id = dtl.OrderId AND orders.BrandId = dtl.BrandId) "+
      "WHERE orders.BrandId = "+ brandid+" and orders.CreateDate > DATE_sub(NOW(),100)) orders_dt1 "+
      "LEFT JOIN tayouhaya_mall_tmp.mall_prd_prod prod ON (orders_dt1.ItemId = prod.Id AND orders_dt1.BrandId = prod.BrandId) WHERE prod.ItemNo > ''"
    val sql_p = "select itemno as itemid from tayouhaya_mall_tmp.mall_prd_prod where brandid = " + brandid
    // 商品行为的时间迄今时间差
    val behavior = spark.sql(sql_buy).withColumnRenamed("vipid","userid").withColumnRenamed("ItemNO","itemI")
      .withColumn("diff", datediff(lit(current_date()), col("daystime")))//.withColumnRenamed("ItemNO","itemno")
    val product = spark.sql(sql_p)
    //数据做关联  去重复购买
    val user_item = product.join(behavior,product("itemid") === behavior("itemI"),"inner").select("userid","itemid")
                           .dropDuplicates("userid","itemid")//.withColumn("score",lit(1))

    // 用户分组，同一个用户购买的商品放一起   (用户：物品) => (用户：(物品集合))
    val user_ds1 =user_item.groupBy("userid").agg(collect_set("itemid"))
                             .withColumnRenamed("collect_set(itemid)", "itemid_set")
    // 物品:物品
    val user_ds2 = user_ds1.flatMap { row =>
      val itemlist = row.getAs[scala.collection.mutable.WrappedArray[String]](1).toArray.sorted
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
    val user_ds0 = user_item.withColumn("score", lit(1)).groupBy("itemid").agg(sum("score").as("score"))
    // 计算同现相似度
    val user_ds4 = user_ds3.join(user_ds0.withColumnRenamed("itemid", "itemidJ").withColumnRenamed("score", "sumJ").select("itemidJ", "sumJ"), "itemidJ")
    val user_ds5 = user_ds4.join(user_ds0.withColumnRenamed("itemid", "itemidI").withColumnRenamed("score", "sumI").select("itemidI", "sumI"), "itemidI")
    // 根据公式N(i)∩N(j)/sqrt(N(i)*N(j)) 计算
    val user_ds6 = user_ds5.withColumn("result", col("sumIJ") / sqrt(col("sumI") * col("sumJ")))
    //合并
    val user_ds8 = user_ds6.select("itemidI", "itemidJ", "result").union(user_ds6.select($"itemidJ".as("itemidI"), $"itemidI".as("itemidJ"), $"result"))

    //      结果转换rdd
//    val out: Dataset[ItemSimi] = user_ds8.select("itemidI", "itemidJ", "result").map { row =>
//      val itemidI = row.getString(0)
//      val itemidJ = row.getString(1)
//      val similar = row.getDouble(2)
//      ItemSimi(itemidI, itemidJ, similar)
//    }
//    out.show()

    val result=user_ds8.withColumnRenamed("result","score")
    //以商品分组，商品分数排序  取前十名
    val result_top= result.withColumn("rank", row_number.over(Window.partitionBy("itemidI").orderBy(col("score").desc)))
                           .where($"rank" <= 10)//.drop("rank")

    //用户购买商品和用户可能买了又买的商品连接关联
    val user_cf = behavior.join(result_top,behavior("ItemI") === result_top("itemidI"),"left")
                           .select("userid","itemidJ","score","diff")//.dropDuplicates("userid","itemidJ")
                           .groupBy("userid", "itemidJ").agg(sum($"score"/$"diff").as("score"))
    val user_cf_rank = user_cf.withColumn("rank", row_number.over(Window.partitionBy("userid").orderBy(col("score").desc)))
                                .where($"rank" <= 30).select("userid","itemidJ","score")
    //写入hive
    spark.sql("truncate table recommend_pro"+brandid+".recall_cf_result")
    user_cf_rank.write.mode(SaveMode.Overwrite).insertInto("recommend_pro"+brandid+".recall_cf_result")
    spark.stop()
  }
}
