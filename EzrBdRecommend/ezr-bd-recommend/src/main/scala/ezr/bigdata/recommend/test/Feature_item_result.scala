package ezr.bigdata.recommend.test

import org.apache.spark.sql.functions.{col, current_date, datediff, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Feature_item_result {
  def main(args: Array[String])= {
    val shardingGrpId: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Feature_item_result pro"+shardingGrpId)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val slot = 15
    import spark.implicits._
      val sql_u = "select vipid as user_id,itemno as item_id,activetype as behavior,daystime from pro37.user_item_behavior"
      val item_user = spark.sql(sql_u)
  //添加一列为给的时间
  val behavior1 = item_user.withColumn("today" ,lit(current_date()))
  //用户行为时间和给的时间做时间差
  val behavior2 = behavior1.withColumn("diff", datediff(col("today"), col("daystime")))
  //  商品 点击/收藏/加购/购买 次数
  var item_count = behavior2.stat.crosstab("item_id","behavior").withColumnRenamed("1" ,"h_1")
    .withColumnRenamed("3" ,"h_3").withColumnRenamed("4" ,"h_4")
  //  商品前 3天 点击/收藏/加购/购买 次数
  val beforethreeday = behavior2.filter($"diff" <=  "3")
  val item_count_before_3 = beforethreeday.stat.crosstab("item_id","behavior").withColumnRenamed("1" ,"i_1")
    .withColumnRenamed("3" ,"i_3").withColumnRenamed("4" ,"i_4")
  //  商品前 2天 点击/收藏/加购/购买 次数
  val  beforetwoday = behavior2.filter($"diff" <=  "2")
  val item_count_before_2 = beforetwoday.stat.crosstab("item_id","behavior").withColumnRenamed("1" ,"j_1")
    .withColumnRenamed("3" ,"j_3").withColumnRenamed("4" ,"j_4")
  //  商品前 1天 点击/收藏/加购/购买 次数
  val  beforeoneday = behavior2.filter($"diff" <=  "1")
  val item_count_before_1 = beforeoneday.stat.crosstab("item_id","behavior").withColumnRenamed("1" ,"k_1")
    .withColumnRenamed("3" ,"k_3").withColumnRenamed("4" ,"k_4")
  //商品 点击/收藏/加购/购买 平均次数
  val countAverage = item_count.select(col("item_id_behavior"),(col("h_1")/slot).alias("l_1")
    ,(col("h_3")/slot).alias("l_3"),(col("h_4")/slot).alias("l_4"))

  //  多种行为特则都存在加购点击购买  未有行为以0表示
  //    for(colum <- item_count.columns.toSet -- item_count_before_3.columns.toSet){item_count_before_3 = item_count_before_3.withColumn(colum,lit(0))}
  //  for(colum <- item_count.columns.toSet -- item_count_before_2.columns.toSet){item_count_before_2 = item_count_before_2.withColumn(colum,lit(0))}
  //  for(colum <- item_count.columns.toSet -- item_count_before_1.columns.toSet){item_count_before_1 = item_count_before_1.withColumn(colum,lit(0))}

  //  特征组合
  item_count = item_count.join(item_count_before_1,item_count_before_1("item_id_behavior")===item_count("item_id_behavior"),"left").drop(item_count_before_1("item_id_behavior"))
  item_count = item_count.join(countAverage,countAverage("item_id_behavior")===item_count("item_id_behavior"),"left").drop(countAverage("item_id_behavior"))
  item_count = item_count.join(item_count_before_3,item_count_before_3("item_id_behavior")===item_count("item_id_behavior"),"left").drop(item_count_before_3("item_id_behavior"))
  item_count = item_count.join(item_count_before_2,item_count_before_2("item_id_behavior")===item_count("item_id_behavior"),"left").drop(item_count_before_2("item_id_behavior"))

    val feature = List("h_1","h_3","h_4","i_1","i_3","i_4","j_1","j_3","j_4","k_1","k_3","k_4","l_1","l_3","l_4")
    for (x<-feature){
      if (item_count.columns.toList.contains(x)){}
      else{item_count = item_count.withColumn(x,lit(0))
      }}
    item_count = item_count.na.fill(0)
    println(item_count.columns.toList)
    //导入数据
    spark.sql("truncate table pro"+shardingGrpId+".feature_item_result")
    item_count.select("item_id_behavior","h_1","h_3","h_4","i_1","i_3","i_4","j_1","j_3","j_4","k_1","k_3","k_4","l_1","l_3","l_4")
      .write.mode(SaveMode.Overwrite).insertInto("pro"+shardingGrpId+".feature_item_result")
    spark.stop()

}
}
