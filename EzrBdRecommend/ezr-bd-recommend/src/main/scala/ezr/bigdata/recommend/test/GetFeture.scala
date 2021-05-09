package ezr.bigdata.recommend.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/17  用户和商品特征函数
  *         #用户自身特征
  *         1.用户一段时间内 点击/加购/购买  次数
  *         2.用户一段时间内 点击/加购/购买  平均次数
  *         3.用户前 1天/2天/3天 点击/加购/购买  次数
  *         4.用户行为距今最大天数，最小小天数，天数差
  *         5.用户点击/加购行为行为活跃的天数
  *         #商品自身特征
  *         1.商品一月内 点击/加购/购买 次数
  *         2.商品一月内 点击/加购/购买 平均次数
  *         3.商品前 1天/2天/3天 点击/加购/购买 次数
  */

object GetFeture {
def user_id_feture(data_user:DataFrame,end_time:Column,slot:Int,spark:SparkSession): DataFrame ={
    import spark.implicits._
//    val sql_u = "select vipid as user_id,itemno as item_id,activetype as behavior,daystime from pro37.user_item_behavior"
//    val behavior = spark.sql(sql_u)
    //添加一列为给的时间
    val behavior1 = data_user.withColumn("today" ,lit(end_time))
    //用户行为时间和给的时间做时间差
    val behavior2 = behavior1.withColumn("diff", datediff(col("today"), col("daystime")))
  //统计用户各种行为的次数
  var user_count = behavior2.stat.crosstab("user_id","behavior").withColumnRenamed("1" ,"a_1")
    .withColumnRenamed("3" ,"a_3").withColumnRenamed("4" ,"a_4")
//统计时间差为三天内的各种行为次数
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
//    user_count = renameDataFrame(user_count,spark:SparkSession).na.fill(0)
   val feature = List("a_1","a_3","a_4","b_1","b_3","b_4","c_1","c_3","c_4","d_1","d_3","d_4",
                      "e_1","e_3","e_4","f_1","f_3","f_4","g_1","g_3","g_4")
  for (x<-feature){
    if (user_count.columns.toList.contains(x)){}
    else{user_count = user_count.withColumn(x,lit(0))
    }}
  user_count = user_count.select("user_id_behavior","a_1","a_3","a_4","b_1","b_3","b_4","c_1","c_3",
                     "c_4","d_1","d_3","d_4","e_1","e_3","e_4","f_1","f_3","f_4","g_1","g_3","g_4").na.fill(0)
  return  user_count
//val user1 = Seq(("A1","10","张三","上海"),("A2","20","李四","北京"),("A3","30","王五","南京")).toDF("id","age","name","address")
//  var user2 = Seq(("A2","张飞"),("A3","李逵")).toDF("id2","name2")
//  user1.join(user2,user1("id")===user2("id2"),"left").na.fill("0").show()
}
def item_id_feture(item_user:DataFrame,end_time:Column,slot:Int,spark:SparkSession): DataFrame = {
    import spark.implicits._
//    val sql_u = "select vipid as user_id,itemno as item_id,activetype as behavior,daystime from pro37.user_item_behavior"
//    val behavior = spark.sql(sql_u)
    //添加一列为给的时间
    val behavior1 = item_user.withColumn("today" ,lit(end_time))
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
    var countAverage = item_count.select(col("item_id_behavior"),(col("h_1")/slot).alias("l_1")
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
//  item_count = renameDataFrame(item_count,spark:SparkSession).na.fill(0)
val feature = List("h_1","h_3","h_4","i_1","i_3","i_4","j_1","j_3","j_4","k_1","k_3","k_4","l_1","l_3","l_4")
  for (x<-feature){
    if (item_count.columns.toList.contains(x)){}
    else{item_count = item_count.withColumn(x,lit(0))
    }}
  item_count = item_count.select("item_id_behavior","h_1","h_3","h_4","i_1","i_3","i_4","j_1","j_3",
                                            "j_4","k_1","k_3","k_4","l_1","l_3","l_4").na.fill(0)

  return item_count
}
  def RenameDataFrame(df:DataFrame,spark:SparkSession): DataFrame ={
    //修改重复列名的名字
    val schema: StructType = df.schema
    val arrSchema = ArrayBuffer[StructField]()

    var map = Map[String,Int]()
    schema.foreach(r=>{
      val schName = r.name
      if(map.isEmpty){
        map +=  schName -> 1
        arrSchema.append(StructField(schName,r.dataType,true))
      }else{
        if(map.contains(schName)){
          val num = map.get(schName).getOrElse(0)
          map += schName -> (num+1)
          arrSchema.append(StructField(schName+"_"+num,r.dataType,true))
        }else{
          map +=  schName -> 1
          arrSchema.append(StructField(schName,r.dataType,true))
        }
      }
    })

    val mySchema: StructType = StructType(arrSchema)
    //    println(mySchema)
    spark.createDataFrame(df.rdd,mySchema)
  }
}
