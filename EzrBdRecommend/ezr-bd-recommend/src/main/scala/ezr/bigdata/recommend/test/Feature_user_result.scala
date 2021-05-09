package ezr.bigdata.recommend.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author wuyuantin@easyretailpro.com
  *         2020/1/6  用户和商品特征函数
  *         #用户自身特征
  *         1.用户一段时间内 点击/加购/购买  次数
  *         2.用户一段时间内 点击/加购/购买  平均次数
  *         3.用户前 1天/2天/3天 点击/加购/购买  次数
  *         4.用户行为距今最大天数，最小小天数，天数差
  *         5.用户点击/加购行为行为活跃的天数
  */
object Feature_user_result {
  def main(args: Array[String])= {
    val shardingGrpId: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Feature_user_result pro"+shardingGrpId)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val slot = 15

    val sql_u = "select vipid as user_id,itemno as item_id,activetype as behavior,daystime from pro"+shardingGrpId+".user_item_behavior"
    val data_user = spark.sql(sql_u)
    //添加一列为给的时间
    val behavior1 = data_user.withColumn("today" ,lit(current_date()))
    //用户行为时间和给的时间做时间差
    val behavior2 = behavior1.withColumn("diff", datediff(col("today"), col("daystime")))
    //统计用户各种行为的次数
    var user_count = behavior2.stat.crosstab("user_id","behavior").withColumnRenamed("1" ,"a_1")
      .withColumnRenamed("3" ,"a_3").withColumnRenamed("4" ,"a_4")
//    //统计时间差为三天内的各种行为次数
    val beforethreeday = behavior2.filter($"diff" <=  "3")
    val user_count_before_3 = beforethreeday.stat.crosstab("user_id","behavior").withColumnRenamed("1" ,"b_1")
      .withColumnRenamed("3" ,"b_3").withColumnRenamed("4" ,"b_4")
    //统计时间差为两天内各行为的次数
    val  beforetwoday = behavior2.filter($"diff" <=  "2")
    val user_count_before_2 = beforetwoday.stat.crosstab("user_id","behavior").withColumnRenamed("1" ,"c_1")
      .withColumnRenamed("3" ,"c_3").withColumnRenamed("4" ,"c_4")
    //统计时间差为一天的各行为次数
    val  beforeoneday = behavior2.filter($"diff" <=  "1")
    val user_count_before_1 = beforeoneday.stat.crosstab("user_id","behavior").withColumnRenamed("1" ,"d_1")
      .withColumnRenamed("3" ,"d_3").withColumnRenamed("4" ,"d_4")
    //各行为平均次数
    val countAverage = user_count.select(col("user_id_behavior"),(col("a_1")/slot).alias("e_1"),
      (col("a_3")/slot).alias("e_3"),(col("a_4")/slot).alias("e_4"))
    //用户行为距今最大天数  最小天数   天数差
    val long_online = behavior2.groupBy("user_id").agg(max("diff").as("f_1"),min("diff").as("f_3")).withColumn("f_4",col("f_1")-col("f_3"))
    //用户存在行为的天数合计
    val user_live = behavior2.dropDuplicates("user_id","behavior","daystime").groupBy("user_id","behavior").agg(count("diff"))
      .groupBy("user_id").pivot("behavior").sum("count(diff)").withColumnRenamed("1" ,"g_1")
      .withColumnRenamed("3" ,"g_3").withColumnRenamed("4" ,"g_4")

    //把各类特征合并
    user_count = user_count.join(user_count_before_1,user_count_before_1("user_id_behavior")===user_count("user_id_behavior"),"left").drop(user_count_before_1("user_id_behavior"))
    user_count = user_count.join(countAverage,countAverage("user_id_behavior")===user_count("user_id_behavior"),"left").drop(countAverage("user_id_behavior"))
    user_count = user_count.join(user_live,user_live("user_id")===user_count("user_id_behavior"),"left").drop(user_live("user_id"))
    user_count = user_count.join(user_count_before_3,user_count_before_3("user_id_behavior")===user_count("user_id_behavior"),"left").drop(user_count_before_3("user_id_behavior"))
    user_count = user_count.join(user_count_before_2,user_count_before_2("user_id_behavior")===user_count("user_id_behavior"),"left").drop(user_count_before_2("user_id_behavior"))
    user_count = user_count.join(long_online,long_online("user_id")===user_count("user_id_behavior"),"left").drop(long_online("user_id"))

   val feature = List("a_1","a_3","a_4","b_1","b_3","b_4","c_1","c_3","c_4","d_1","d_3","d_4",
                 "e_1","e_3","e_4","f_1","f_3","f_4","g_1","g_3","g_4")
    for (x<-feature){
      if (user_count.columns.toList.contains(x)){}
        else{user_count = user_count.withColumn(x,lit(0))
      }}
    user_count = user_count.na.fill(0)
    println(user_count.columns.toList)
    //导入数据
    spark.sql("truncate table pro"+shardingGrpId+".feature_user_result")
    user_count.select("user_id_behavior","a_1","a_3","a_4","b_1","b_3","b_4","c_1","c_3","c_4","d_1","d_3","d_4","e_1","e_3","e_4","f_1","f_3","f_4","g_1","g_3","g_4")
      .write.mode(SaveMode.Overwrite).insertInto("pro"+shardingGrpId+".feature_user_result")
    spark.stop()
  }
}
