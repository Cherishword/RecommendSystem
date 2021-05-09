package ezr.bigdata.recommend.Rank

import ezr.bigdata.recommend.Model.GetFeture
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.joda.time.DateTime

object Rank_ctr_item {
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    val daytime = args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Rank_ctr_item pro "+brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //   选取召回的商品合并
    val sql_sim = "select user_id,similar_behavior_item item_id from recommend_pro.recall_sim_behavior where brandid ="+brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
    val sql_browse = "select user_id,similar_browse_item item_id from recommend_pro.recall_cf_browse where brandid ="+brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
    val sql_cart = "select user_id,similar_cart_item item_id from recommend_pro.recall_cf_cart where brandid ="+brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
    val sql_buy = "select user_id,similar_buy_item item_id from recommend_pro.recall_cf_buy where brandid ="+brandid +" and dt=regexp_replace('"+daytime+"', '-', '')"
    val sql_behavior ="select user_id,item_id,behavior_time,ExposeCount,TriggerCount,BrowseCount,CartCount,BuyCount from recommend_pro.base_behavior_count  where brandid = " + brandid +" and dt>regexp_replace(date_sub('"+daytime+"',30), '-', '')"
    val recall_sim = spark.sql(sql_sim)
    val recall_browse = spark.sql(sql_browse)
    val recall_cart = spark.sql(sql_cart)
    val recall_buy = spark.sql(sql_buy)

    val recall_behavior = spark.sql(sql_behavior).where($"ExposeCount">0).filter($"TriggerCount"<=$"ExposeCount")
//    从表中得到召回用户和商品
    val recall_item = recall_sim.union(recall_browse).union(recall_cart).union(recall_buy).dropDuplicates("user_id", "item_id")
//    和用户行为、商品行为匹配
    val recall_feture_user = recall_item.dropDuplicates("user_id").join(recall_behavior,Seq("user_id"),"inner")
    val recall_feture_item = recall_item.dropDuplicates("item_id").join(recall_behavior,Seq("item_id"),"inner")
    // 得到用户特征和商品特征
    val user_id_feture = GetFeture.user_id_feture(spark,recall_feture_user,daytime,30)
    val item_id_feture = GetFeture.item_id_feture(spark,recall_feture_item,daytime,30)
    //把用户特征和商品特征做关联
    var x = recall_item.join(user_id_feture,Seq("user_id"),"left")
    x = x.join(item_id_feture,Seq("item_id"),"left").na.fill(0)
//    for (col<-x.columns){
//      x = x.withColumn(col, when(x(col).isNull, 0).otherwise(x(col)))}
    //选择和转化输入模型的特征为Double类型
    val rank = x.select(x.drop("user_id", "item_id").columns.map(f => col(f).cast(DoubleType)): _*)
    //选出最新时间的模型路径
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val PathName = fs.listStatus(new Path(s"/data/lrmodel/"+brandid))
    val model_time =PathName.filter(_.getModificationTime==PathName.map(_.getModificationTime).max).map(_.getPath).mkString("")
    println(model_time)
    //加载模型
    val load_lrModel = LogisticRegressionModel.load(model_time)
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
    val click_id = x.select("user_id", "item_id").withColumn("id", monotonically_increasing_id())
    val click_pro = click_id.join(pro,Seq("id"),"left" )
    //取前三十名的商品
    val click_rank = click_pro.withColumn("rank", row_number.over(Window.partitionBy("user_id")
      .orderBy(col("pro").desc))).where($"rank" <= 30)//.drop("rank")
      .withColumn("pro",$"pro"*100)

    click_rank.repartition(2).persist().createOrReplaceTempView("table")
    val sql1 =  "alter table recommend_pro.rank_ctr_item drop IF EXISTS partition(brandid = "+brandid+",dt="+daytime.replace("-","")+")"
    val sql2 = "insert into table recommend_pro.rank_ctr_item partition(brandid = "+brandid+",dt="+daytime.replace("-","")+") select user_id,item_id,pro,rank from table"
    spark.sql(sql1)
    spark.sql(sql2)

    val beforeHundredDay = new DateTime(daytime).minusDays(3).toString("yyyMMdd")
    spark.sql(s"alter table recommend_pro.rank_ctr_item drop if exists partition(brandid = $brandid,dt=$beforeHundredDay)")

  }
}
