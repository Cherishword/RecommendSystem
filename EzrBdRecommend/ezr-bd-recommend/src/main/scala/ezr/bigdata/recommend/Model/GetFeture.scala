package ezr.bigdata.recommend.Model

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/17  用户和商品特征函数
  *         #用户自身特征
  *         1.用户一段时间内 曝光/触发/点击/加购/购买  次数
  *         2.用户一段时间内 曝光/触发/点击/加购/购买  平均次数
  *         3.用户前 1天/2天/3天 曝光/触发/点击/加购/购买  次数
  *         4.用户行为距今最大天数，最小小天数，天数差
  *         5.用户曝光/触发/点击/加购行为行为活跃的天数
  *         #商品自身特征
  *         1.商品一月内 曝光/触发/点击/加购/购买 次数
  *         2.商品一月内 曝光/触发/点击/加购/购买 平均次数
  *         3.商品前 1天/2天/3天 曝光/触发/点击/加购/购买 次数
  */

object GetFeture {
  def user_id_feture(spark:SparkSession,data_user:DataFrame,end_time:String,slot:Int): DataFrame ={
    import spark.implicits._
    //用户行为时间和给的时间做时间差
    val user_day_dif = data_user.withColumn("diff", datediff(lit(end_time), col("behavior_time")))
    //统计用户一月各种行为的次数
    var user_count =  user_day_dif.groupBy("user_id").agg(sum("ExposeCount"),sum("TriggerCount"),
                     sum("BrowseCount"),sum("CartCount"),sum("BuyCount"))
      .withColumnRenamed("sum(ExposeCount)","a_1").withColumnRenamed("sum(TriggerCount)","a_2")
      .withColumnRenamed("sum(BrowseCount)","a_3").withColumnRenamed("sum(CartCount)","a_4")
      .withColumnRenamed("sum(BuyCount)","a_5")
    //统计时间差为三天内的各种行为次数
    val user_count_before_3 = user_day_dif.filter($"diff" <=  "3")
      .groupBy("user_id").agg(sum("ExposeCount"),sum("TriggerCount"),
      sum("BrowseCount"),sum("CartCount"),sum("BuyCount"))
      .withColumnRenamed("sum(ExposeCount)","b_1").withColumnRenamed("sum(TriggerCount)","b_2")
      .withColumnRenamed("sum(BrowseCount)","b_3").withColumnRenamed("sum(CartCount)","b_4")
      .withColumnRenamed("sum(BuyCount)","b_5")
    //统计时间差为两天内各行为的次数
    val  user_count_before_2 = user_day_dif.filter($"diff" <=  "2")
      .groupBy("user_id").agg(sum("ExposeCount"),sum("TriggerCount"),
      sum("BrowseCount"),sum("CartCount"),sum("BuyCount"))
      .withColumnRenamed("sum(ExposeCount)","c_1").withColumnRenamed("sum(TriggerCount)","c_2")
      .withColumnRenamed("sum(BrowseCount)","c_3").withColumnRenamed("sum(CartCount)","c_4")
      .withColumnRenamed("sum(BuyCount)","c_5")
    //统计时间差为一天的各行为次数
    val  user_count_before_1 = user_day_dif.filter($"diff" <=  "1")
      .groupBy("user_id").agg(sum("ExposeCount"),sum("TriggerCount"),
      sum("BrowseCount"),sum("CartCount"),sum("BuyCount"))
      .withColumnRenamed("sum(ExposeCount)","d_1").withColumnRenamed("sum(TriggerCount)","d_2")
      .withColumnRenamed("sum(BrowseCount)","d_3").withColumnRenamed("sum(CartCount)","d_4")
      .withColumnRenamed("sum(BuyCount)","d_5")
    //各行为平均次数
    val countAverage = user_count.select(col("user_id"),(col("a_1").cast(StringType)/slot).alias("e_1"),(col("a_2").cast(StringType)/slot).alias("e_2"),
      (col("a_3").cast(StringType)/slot).alias("e_3"),(col("a_4").cast(StringType)/slot).alias("e_4"),(col("a_5").cast(StringType)/slot).alias("e_5"))
    //用户行为距今最大天数  最小天数   天数差
    val long_online = user_day_dif.groupBy("user_id").agg(max("diff").as("f_1"),min("diff").as("f_2")).withColumn("f_3",col("f_1")-col("f_2"))
    //用户存在行为的天数合计
    val user_live = user_day_dif.dropDuplicates("user_id","behavior_time")
    .groupBy("user_id").agg(count("ExposeCount").as("g_1"),count("TriggerCount").as("g_2")
    ,count("BrowseCount").as("g_3"),count("CartCount").as("g_4"),count("BuyCount").as("g_5"))

    //把各类特征合并
    user_count = user_count.join(user_count_before_1,Seq("user_id"),"left")
    user_count = user_count.join(countAverage,Seq("user_id"),"left")
    user_count = user_count.join(user_live,Seq("user_id"),"left")
    user_count = user_count.join(user_count_before_3,Seq("user_id"),"left")
    user_count = user_count.join(user_count_before_2,Seq("user_id"),"left")
    user_count = user_count.join(long_online,Seq("user_id"),"left")

    val feature = List("a_1","a_2","a_3","a_4","a_5","b_1","b_2","b_3","b_4","b_5","c_1","c_2","c_3","c_4","c_5","d_1","d_2","d_3","d_4","d_5",
      "e_1","e_2","e_3","e_4","e_5","f_1","f_2","f_3","g_1","g_2","g_3","g_4","g_5")
    for (x<-feature){
      if (user_count.columns.toList.contains(x)){}
      else{user_count = user_count.withColumn(x,lit(0))
      }}
    user_count = user_count.select("user_id","a_1","a_2","a_3","a_4","a_5","b_1","b_2","b_3","b_4","b_5","c_1","c_2","c_3","c_4","c_5","d_1","d_2","d_3","d_4","d_5",
      "e_1","e_2","e_3","e_4","e_5","f_1","f_2","f_3","g_1","g_2","g_3","g_4","g_5").na.fill(0)
//    for (col<-user_count.columns){user_count = user_count.withColumn(col, when(user_count(col).isNull, 0).otherwise(user_count(col)))}
    return  user_count
    //val user1 = Seq(("A1","10","张三","上海"),("A2","20","李四","北京"),("A3","30","王五","南京")).toDF("id","age","name","address")
    //  var user2 = Seq(("A2","张飞"),("A3","李逵")).toDF("id2","name2")
    //  user1.join(user2,user1("id")===user2("id2"),"left").na.fill("0").show()
  }
  def item_id_feture(spark:SparkSession,data_item:DataFrame,end_time:String,slot:Int): DataFrame = {
    import spark.implicits._
    //用户行为时间和给的时间做时间差
    val behavior_item_dif = data_item.withColumn("diff", datediff(lit(end_time), col("behavior_time")))
    //  商品 曝光/触发/点击/收藏/加购/购买 次数
    var item_count = behavior_item_dif.groupBy("item_id").agg(sum("ExposeCount"),sum("TriggerCount"),
      sum("BrowseCount"),sum("CartCount"),sum("BuyCount"))
      .withColumnRenamed("sum(ExposeCount)","h_1").withColumnRenamed("sum(TriggerCount)","h_2")
      .withColumnRenamed("sum(BrowseCount)","h_3").withColumnRenamed("sum(CartCount)","h_4")
      .withColumnRenamed("sum(BuyCount)","h_5")
    //  商品前 3天  曝光/触发/点击/收藏/加购/购买 次数
    val beforethreeday = behavior_item_dif.filter($"diff" <=  "3")
      .groupBy("item_id").agg(sum("ExposeCount"),sum("TriggerCount"),
      sum("BrowseCount"),sum("CartCount"),sum("BuyCount"))
      .withColumnRenamed("sum(ExposeCount)","i_1").withColumnRenamed("sum(TriggerCount)","i_2")
      .withColumnRenamed("sum(BrowseCount)","i_3").withColumnRenamed("sum(CartCount)","i_4")
      .withColumnRenamed("sum(BuyCount)","i_5")
    //  商品前 2天  曝光/触发/点击/收藏/加购/购买 次数
    val  beforetwoday = behavior_item_dif.filter($"diff" <=  "2")
      .groupBy("item_id").agg(sum("ExposeCount"),sum("TriggerCount"),
      sum("BrowseCount"),sum("CartCount"),sum("BuyCount"))
      .withColumnRenamed("sum(ExposeCount)","j_1").withColumnRenamed("sum(TriggerCount)","j_2")
      .withColumnRenamed("sum(BrowseCount)","j_3").withColumnRenamed("sum(CartCount)","j_4")
      .withColumnRenamed("sum(BuyCount)","j_5")
    //  商品前 1天  曝光/触发/点击/收藏/加购/购买 次数
    val  beforeoneday = behavior_item_dif.filter($"diff" <=  "1")
      .groupBy("item_id").agg(sum("ExposeCount"),sum("TriggerCount"),
      sum("BrowseCount"),sum("CartCount"),sum("BuyCount"))
      .withColumnRenamed("sum(ExposeCount)","k_1").withColumnRenamed("sum(TriggerCount)","k_2")
      .withColumnRenamed("sum(BrowseCount)","k_3").withColumnRenamed("sum(CartCount)","k_4")
      .withColumnRenamed("sum(BuyCount)","k_5")
    //商品  曝光/触发/点击/收藏/加购/购买 平均次数
    var countAverage = item_count.select(col("item_id"),(col("h_1").cast(StringType)/slot).alias("l_1"),
      (col("h_2").cast(StringType)/slot).alias("l_2"), (col("h_3").cast(StringType)/slot).alias("l_3"),
      (col("h_4").cast(StringType)/slot).alias("l_4"),(col("h_5").cast(StringType)/slot).alias("l_5"))
   //  特征组合
    item_count = item_count.join(beforeoneday,Seq("item_id"),"left")
    item_count = item_count.join(countAverage,Seq("item_id"),"left")
    item_count = item_count.join(beforetwoday,Seq("item_id"),"left")
    item_count = item_count.join(beforethreeday,Seq("item_id"),"left")

    val feature = List("h_1","h_2","h_3","h_4","h_5","i_1","i_2","i_3","i_4","i_5","j_1","j_2","j_3","j_4","j_5",
      "k_1","k_2","k_3","k_4","k_5","l_1","l_2","l_3","l_4","l_5")
    for (x<-feature){
      if (item_count.columns.toList.contains(x)){}
      else{item_count = item_count.withColumn(x,lit(0))
      }}
    item_count = item_count.select("item_id","h_1","h_2","h_3","h_4","h_5","i_1","i_2","i_3","i_4","i_5",
      "j_1","j_2","j_3","j_4","j_5","k_1","k_2","k_3","k_4","k_5","l_1","l_2","l_3","l_4","l_5").na.fill(0)
//    for (col<-item_count.columns){item_count = item_count.withColumn(col, when(item_count(col).isNull, 0).otherwise(item_count(col)))}
    return item_count
  }


}
