package ezr.bigdata.recommend.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.DoubleType
import org.elasticsearch.spark._
/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/20  Hive数据表ezr_items_filter上传Es
  */

object HiveToEs {
  case class HiveTableHot(id:String,item_id:String ,item_name:String,saleprice:Double,rank:Long)
  case class HiveTableClick(id:String,vipid:Long ,activetype:String,itemno:String,daystime:String,name:String,saleprice:Double)
  case class HiveTableRec(id:String,user_id:Int ,item_id:String,item_name:String,click_prob:Float,rank:Int,cate_rank:Int,saleprice:String)
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    val esIp =  "172.21.16.32"
    val esPort = "9200"
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("  pro"+brandid)
//      .master("local[*]")
      .config("es.nodes", esIp)
      .config("es.port", esPort)
      .enableHiveSupport()
      .getOrCreate()

    //读取过滤、用户行为、商品详情、热门数据
    val sql_f = "select user_id,item_id,item_name, saleprice,click_prob,rank,cate_rank from recommend_pro"+brandid+".filter_history_recommend"
    val sql_buy ="SELECT orders_dt1.BuyerId vipid, 4 activetype, prod.ItemNo, orders_dt1.daystime "+
      "FROM (SELECT orders.BrandId, orders.BuyerId, DATE_FORMAT(orders.CreateDate,'yyyy-MM-dd') daystime, dtl.ItemId "+
      "FROM tayouhaya_mall_tmp.mall_sales_order orders LEFT JOIN tayouhaya_mall_tmp.mall_sales_order_dtl dtl ON (orders.Id = dtl.OrderId AND orders.BrandId = dtl.BrandId) "+
      "WHERE orders.BrandId = "+ brandid+" and orders.CreateDate > DATE_sub(NOW(),100)) orders_dt1 "+
      "LEFT JOIN tayouhaya_mall_tmp.mall_prd_prod prod ON (orders_dt1.ItemId = prod.Id AND orders_dt1.BrandId = prod.BrandId) WHERE prod.ItemNo > ''"
    val sql_cart = "select vipid,1 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime from tayouhaya_youshu_tmp.add_to_cart where brandid = " + brandid +" and dt>20191107"
    val sql_click = "SELECT vipid,3 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime  FROM tayouhaya_youshu_tmp.browse_sku_page where brandid = " + brandid +" and dt>20191107 "
    val sql_p = "select itemNo as itemno,name ,saleprice from tayouhaya_mall_tmp.mall_prd_prod"
    val sql_h = "select item_id ,item_hot from recommend_pro"+brandid+".recall_hot_result order by item_hot desc limit 20"
    val filter = spark.sql(sql_f)
    val behavior = spark.sql(sql_buy).union(spark.sql(sql_cart)).union(spark.sql(sql_click))
    val product = spark.sql(sql_p)
    val hot = spark.sql(sql_h)

    //连接用户历史数据和商品数据  转化数据类型   替换字符
    val user_history_click = behavior.join(product,behavior("itemno")===product("itemno"),"left").drop(product("itemno"))
                            .select(col("vipid"),col("activetype"),col("itemno"),col("daystime"),col("name"),col("saleprice").cast(DoubleType))
      .withColumn("activetype", col("activetype").cast("string")).na.replace("activetype"::Nil, Map("1"->"点击" ,"2"->"收藏","3"->"加购" ,"4"->"购买"))
    user_history_click.show()
    //  用户历史数据导入到ES
    val ToEs_Click = user_history_click.rdd.map(r=>{
      val vipid = r.getAs[Long]("vipid")
      val activetype = r.getAs[String]("activetype")
      val itemno = r.getAs[String]("itemno")
      val daystime = r.getAs[String]("daystime")
      val name = r.getAs[String]("name")
      val saleprice = r.getAs[Double]("saleprice")
      val id = "%s_%s_%s".format(vipid,itemno,daystime)
      HiveTableClick(id,vipid,activetype,itemno,daystime,name,saleprice)
    }).saveToEs("recommend57"  + "/user_history_click", Map("es.mapping.id" -> "id"))

    //  用户过滤后的推荐数据导入到ES
    val ToEs_Rec = filter.rdd.map(r=>{
      val user_id = r.getAs[Int]("user_id")
      val item_id = r.getAs[String]("item_id")
      val item_name = r.getAs[String]("item_name")
      val click_prob = r.getAs[Float]("click_prob")
      val rank = r.getAs[Int]("rank")
      val cate_rank = r.getAs[Int]("cate_rank")
      val saleprice = r.getAs[String]("saleprice")
      val id = "%s_%s".format(user_id,item_id)
      HiveTableRec(id,user_id,item_id,item_name,click_prob,rank,cate_rank,saleprice)
    }).saveToEs("recommend57"  + "/user_rec", Map("es.mapping.id" -> "id"))

    //连接热门数据和商品数据   重命名
    var item_hot = hot.join(product,hot("item_id") === product("itemno"),"left")
            .select(col("item_id"),col("item_hot"),col("name").as("item_name"),col("saleprice").cast(DoubleType))
    //以热度值排序
    item_hot = item_hot.orderBy(-item_hot("item_hot")).withColumn("rank", monotonically_increasing_id())
    //  数据导入到ES
     val ToEs_Hot = item_hot.rdd.map(r=>{
      val item_id = r.getAs[String]("item_id")
      val rank = r.getAs[Long]("rank")
      val item_name = r.getAs[String]("item_name")
      val saleprice = r.getAs[Double]("saleprice")
      val id = "%s_%s".format(item_id,rank)
//       spark.sql("truncate table pro37.ezr_items_")
      HiveTableHot(id,item_id,item_name,saleprice,rank)
    }).saveToEs("recommend57"  + "/item_hot", Map("es.mapping.id" -> "id"))

  }
}
