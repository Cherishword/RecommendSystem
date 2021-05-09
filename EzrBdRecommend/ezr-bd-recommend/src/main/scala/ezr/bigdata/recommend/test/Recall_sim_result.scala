package ezr.bigdata.recommend.test

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/12.
  *         商品间相关系数计算
  */
object Recall_sim_result {
  def main(args: Array[String]): Unit = {
    val brandid: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Recall_sim_result pro"+brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //读取商品和行为数据
    val sql_p = "select itemi,itemj,score,rank from recommend_pro"+brandid+".recall_sim_item"
    val sql_cart = "select vipid,1 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime from tayouhaya_youshu_tmp.add_to_cart where brandid = " + brandid +" and dt>20191107"
    val sql_click = "SELECT vipid,3 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime  FROM tayouhaya_youshu_tmp.browse_sku_page where brandid = " + brandid +" and dt>20191107 "
      // 商品行为的时间迄今时间差
      val behavior =spark.sql(sql_cart).union(spark.sql(sql_click)).withColumnRenamed("spu_id","ItemI")
                             .withColumn("diff", datediff(lit(current_date()), col("daystime")))
    val product_top = spark.sql(sql_p)

    //用户点击的商品和相似商品做关联，推荐相似的商品给用户
    val user_sim = behavior.join(product_top,behavior("ItemI") === product_top("ItemI"),"left").select("vipid","ItemJ","score","diff")
                      .groupBy("vipid", "ItemJ").agg(sum($"score"/$"diff").as("score"))
    val user_sim_rank = user_sim.withColumn("rank", row_number.over(Window.partitionBy("vipid").orderBy(col("score").desc)))
                                     .where($"rank" <= 30).select("vipid","ItemJ","score")
    //导入数据
    spark.sql("truncate table recommend_pro"+brandid+".recall_sim_result")
    user_sim_rank.write.mode(SaveMode.Overwrite).insertInto("recommend_pro"+brandid+".recall_sim_result")
    spark.stop()
  }
}
