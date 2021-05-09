package ezr.bigdata.recommend.Save

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.elasticsearch.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ezr.common.mybatis.controller.OptBdShardCfgController

import scala.collection.mutable.WrappedArray
object Result_save_es {
  case class HiveTableHot(id:String,brandid:Int,date:Int,rank:Int,spuid:String,Total_BrowseCount: Int,Total_CartCount: Int,Total_BuyCount: Int,Total_Ctr :Float,Total_Cvr:Float,Total_HotValue:String,
       TwoMonth_BrowseCount: Int,TwoMonth_CartCount: Int,TwoMonth_BuyCount: Int,TwoMonth_Ctr:Float,TwoMonth_Cvr:Float,TwoMonth_HotValue:String,
       OneMonth_BrowseCount: Int,OneMonth_CartCount: Int,OneMonth_BuyCount: Int,OneMonth_Ctr:Float,OneMonth_Cvr:Float,OneMonth_HotValue:String,
       OneWeek_BrowseCount: Int,OneWeek_CartCount: Int,OneWeek_BuyCount: Int,OneWeek_Ctr:Float,OneWeek_Cvr:Float,OneWeek_HotValue:String,
       OneDay_BrowseCount: Int,OneDay_CartCount: Int,OneDay_BuyCount: Int,OneDay_Ctr :Float,OneDay_Cvr:Float,OneDay_HotValue:String)
  case class HiveTableOffline(id:String,brandid:Int,date:Int,vipid:Int ,spuids:WrappedArray[String])
  def main(args: Array[String]):Unit= {
    val shardingGrp = args(0)
    val brandid: String = args(1)
    val daytime = args(2)
    val tomorrow = args(3)  //因业务需求，传到es数据时间+1天
    val esIp =  "172.21.17.30,172.21.17.77,172.21.17.199,172.21.17.108,172.21.17.80"
    val esPort = "9200"

//    val dataCenter: String = OptBdShardCfgController.getDataCenter(shardingGrp.toInt)
//    //根据shardingGrpId获取es服务器地址
//    val servers: Array[String] = OptBdShardCfgController.getESServers(shardingGrp.toInt,dataCenter)
//    val esIp = servers(0)
//    val esPort = servers(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SavetoEs pro "+brandid)
//      .master("local[*]")
      .config("es.nodes", esIp)
      .config("es.port", esPort)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //读取过滤的商品数据、热门商品数据
 val sql_hot = "select item_id,total_browsecount, total_cartcount,total_buycount,total_ctr,total_cvr,cast(total_hotvalue as string)," +
              "twomonth_browsecount,twomonth_cartcount,twomonth_buycount, twomonth_ctr,twomonth_cvr,cast(twomonth_hotvalue as string), " +
              "onemonth_browsecount,onemonth_cartcount,onemonth_buycount,onemonth_ctr,onemonth_cvr,cast(onemonth_hotvalue as string), " +
              "oneweek_browsecount,oneweek_cartcount, oneweek_buycount,oneweek_ctr,oneweek_cvr,cast(oneweek_hotvalue as string)," +
              "oneday_browsecount,oneday_cartcount,oneday_buycount,oneday_ctr,oneday_cvr,cast(oneday_hotvalue as string) " +
              "from recommend_pro.recall_hot_item where brandid = " + brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
 val sql_filter = "select user_id,item_id,rank from recommend_pro.filter_history_item where brandid = " + brandid +" and dt=regexp_replace('"+daytime+"', '-', '') order by user_id,rank"
    val filter = spark.sql(sql_filter).groupBy("user_id").agg(collect_list("item_id"))
    val hot = spark.sql(sql_hot).withColumn("rank", row_number.over(Window.orderBy(col("onemonth_hotvalue").desc)))

    //  过滤后离线推荐数据导入到ES
    val ToEs_Filter = filter.rdd.map(r=>{
      val date = tomorrow.replace("-","").toInt
      val vipid = r.getAs[String]("user_id").toInt
      val spuids = r.getAs[WrappedArray[String]]("collect_list(item_id)")
      val id = "%s_%s".format(brandid,vipid)
      HiveTableOffline(id,brandid.toInt,date,vipid,spuids)
    }).saveToEs("recommend_offline" +shardingGrp + "/recommendofflineinfo", Map("es.mapping.id" -> "id"))

    //热门商品数据导入到ES
    val ToEs_Hot = hot.rdd.map(r=>{
      val spuid = r.getAs[String]("item_id")
      val date = tomorrow.replace("-","").toInt
      val rank = r.getAs[Int]("rank")
      val id = "%s_%s".format(brandid,rank)
      val total_browsecount=r.getAs[Int]("total_browsecount")
      val total_cartcount=r.getAs[Int]("total_cartcount")
      val total_buycount=r.getAs[Int]("total_buycount")
      val total_ctr=r.getAs[Float]("total_ctr")
      val total_cvr=r.getAs[Float]("total_cvr")
      val total_hotvalue=r.getAs[String]("total_hotvalue")

      val twomonth_browsecount=r.getAs[Int]("twomonth_browsecount")
      val twomonth_cartcount=r.getAs[Int]("twomonth_cartcount")
      val twomonth_buycount=r.getAs[Int]("twomonth_buycount")
      val twomonth_ctr=r.getAs[Float]("twomonth_ctr")
      val twomonth_cvr=r.getAs[Float]("twomonth_cvr")
      val twomonth_hotvalue=r.getAs[String]("twomonth_hotvalue")

      val onemonth_browsecount=r.getAs[Int]("onemonth_browsecount")
      val onemonth_cartcount=r.getAs[Int]("onemonth_cartcount")
      val onemonth_buycount=r.getAs[Int]("onemonth_buycount")
      val onemonth_ctr=r.getAs[Float]("onemonth_ctr")
      val onemonth_cvr=r.getAs[Float]("onemonth_cvr")
      val onemonth_hotvalue=r.getAs[String]("onemonth_hotvalue")

      val oneweek_browsecount=r.getAs[Int]("oneweek_browsecount")
      val oneweek_cartcount=r.getAs[Int]("oneweek_cartcount")
      val oneweek_buycount=r.getAs[Int]("oneweek_buycount")
      val oneweek_ctr=r.getAs[Float]("oneweek_ctr")
      val oneweek_cvr=r.getAs[Float]("oneweek_cvr")
      val oneweek_hotvalue=r.getAs[String]("oneweek_hotvalue")

      val oneday_browsecount=r.getAs[Int]("oneday_browsecount")
      val oneday_cartcount=r.getAs[Int]("oneday_cartcount")
      val oneday_buycount=r.getAs[Int]("oneday_buycount")
      val oneday_ctr=r.getAs[Float]("oneday_ctr")
      val oneday_cvr=r.getAs[Float]("oneday_cvr")
      val oneday_hotvalue=r.getAs[String]("oneday_hotvalue")

      HiveTableHot(id,brandid.toInt,date,rank,spuid,total_browsecount,total_cartcount,total_buycount,total_ctr,total_cvr,total_hotvalue,
        twomonth_browsecount,twomonth_cartcount,twomonth_buycount,twomonth_ctr,twomonth_cvr,twomonth_hotvalue,
        onemonth_browsecount,onemonth_cartcount,onemonth_buycount,onemonth_ctr,onemonth_cvr,onemonth_hotvalue,
        oneweek_browsecount,oneweek_cartcount,oneweek_buycount,oneweek_ctr,oneweek_cvr,oneweek_hotvalue,
        oneday_browsecount,oneday_cartcount,oneday_buycount,oneday_ctr,oneday_cvr,oneday_hotvalue)
    }).saveToEs("recommend_hot" +shardingGrp + "/recommendhotinfo", Map("es.mapping.id" -> "id"))
  }
}
