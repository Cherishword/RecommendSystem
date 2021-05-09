package ezr.bigdata.recommend.test

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_date, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.{DateType, DoubleType}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/19  导入LR模型计算召回的商品被用户点击概率
  */

object Rank_ctr_result {
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Rank_ctr_result pro"+brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //读取数据  把商品和用户点击行为做连接
    val sql_buy ="SELECT orders_dt1.BuyerId vipid, 4 activetype, prod.ItemNo, orders_dt1.daystime "+
      "FROM (SELECT orders.BrandId, orders.BuyerId, DATE_FORMAT(orders.CreateDate,'yyyy-MM-dd') daystime, dtl.ItemId "+
      "FROM tayouhaya_mall_tmp.mall_sales_order orders LEFT JOIN tayouhaya_mall_tmp.mall_sales_order_dtl dtl ON (orders.Id = dtl.OrderId AND orders.BrandId = dtl.BrandId) "+
      "WHERE orders.BrandId = "+ brandid+" and orders.CreateDate > DATE_sub(NOW(),100)) orders_dt1 "+
      "LEFT JOIN tayouhaya_mall_tmp.mall_prd_prod prod ON (orders_dt1.ItemId = prod.Id AND orders_dt1.BrandId = prod.BrandId) WHERE prod.ItemNo > ''"
    val sql_cart = "select vipid,1 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime from tayouhaya_youshu_tmp.add_to_cart where brandid = " + brandid +" and dt>20191107"
    val sql_click = "SELECT vipid,3 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime  FROM tayouhaya_youshu_tmp.browse_sku_page where brandid = " + brandid +" and dt>20191107 "
    val sql_p = "select itemno as item_id,name as item_name,categoryid,cuscategoryid,saleprice from tayouhaya_mall_tmp.mall_prd_prod"
    val behavior = spark.sql(sql_buy).union(spark.sql(sql_cart)).union(spark.sql(sql_click)).withColumnRenamed("vipid","user_id")
            .withColumnRenamed("itemno","item_idI").withColumnRenamed("activetype","behavior")
            .withColumn("daystime",col("daystime").cast(DateType))
    val product = spark.sql(sql_p)
    val user_item = behavior.join(product,product("item_id") === behavior("item_idI"),"inner")
        .drop("item_idI")//.na.replace("behavior" ::Nil, Map("3" -> "4","4"->"10"))
//   选取召回的商品合并
    val sql_s = "select user_id,item_id from recommend_pro"+brandid+".recall_sim_result"
    val sql_t = "select user_id,item_id from recommend_pro"+brandid+".recall_tag_result"
    val sql_c = "select user_id,item_id from recommend_pro"+brandid+".recall_cf_result"
    val recall_sim = spark.sql(sql_s)
    val recall_tag = spark.sql(sql_t)
    val recall_cf = spark.sql(sql_c)
    var recall_item = recall_sim.union(recall_tag).union(recall_cf).dropDuplicates("user_id","item_id")
//获取有行为的用户和商品特征  为后面得到商品特征和用户特征做准备
    val data_user = recall_item.join(user_item,user_item("user_id") === recall_item("user_id"),"inner")
                           .drop(recall_item("user_id")).drop(recall_item("item_id"))
    val data_item = recall_item.join(user_item,user_item("item_id") === recall_item("item_id"),"inner")
      .drop(recall_item("user_id")).drop(recall_item("item_id"))
// 得到用户特征和商品特征
    val slot = 15
//    val end_time = new DateTime("2019-11-09").toString("yyyy-MM-dd")
    val end_time = current_date()
    val user_id_feture = GetFeture.user_id_feture(data_user,end_time,slot,spark)
    val item_id_feture = GetFeture.item_id_feture(data_item,end_time,slot,spark)
//把用户特征和商品特征与召回的用户商品做连接
    var x = recall_item.join(user_id_feture,user_id_feture("user_id_behavior")===recall_item("user_id"),"left").drop(user_id_feture("user_id_behavior"))
    x = x.join(item_id_feture,item_id_feture("item_id_behavior")===x("item_id"),"left").drop(item_id_feture("item_id_behavior"))
    x = x.join(product,product("item_id")===x("item_id"),"left").drop(product("item_id")).na.fill(0)
    //重命名DataFrame列名并把null替换为0
//    x = GetFeture.RenameDataFrame(x,spark).na.fill(0)
    //选择和转化输入模型的特征为Double类型
    val rank = x.select(x.drop("user_id", "item_id" ,"item_name","categoryid" , "cuscategoryid", "saleprice")
                           .columns.map(f => col(f).cast(DoubleType)): _*)
    //加载模型
    val load_lrModel = LogisticRegressionModel.load(s"/data/lrmodel/20191218")
    //合并输入的特征
    val vectorAssembler = new VectorAssembler().
      setInputCols(rank.columns).
      setOutputCol("features")
    //预测输入的结果
    val result = load_lrModel.transform(vectorAssembler.transform(rank))
//提取预测点击的概率
    val pro = result.map(line => {
      val dense = line.get(line.fieldIndex("probability")).asInstanceOf[org.apache.spark.ml.linalg.DenseVector]
      val y = dense(1).toString
      (y)}).toDF("pro").select(col("pro").cast(DoubleType))
      .withColumn("id", monotonically_increasing_id())
    //把用户商品和点击概率做拼接
    val click_id = x.select("user_id", "item_id" ,"item_name","categoryid" , "cuscategoryid", "saleprice")
           .withColumn("id", monotonically_increasing_id())
    val click_pro = click_id.join(pro,click_id("id") === pro("id"),"left" ).drop("id")
    //取前三十名的商品
    val click_rank = click_pro.withColumn("rank", row_number.over(Window.partitionBy("user_id")
                   .orderBy(col("pro").desc))).where($"rank" <= 30)//.drop("rank")
   //数据导入数据库
    spark.sql("truncate table recommend_pro"+brandid+".rank_ctr_result")
    click_rank.write.mode(SaveMode.Overwrite).insertInto("recommend_pro"+brandid+".rank_ctr_result")
    spark.stop()

  }

}
