package ezr.bigdata.recommend.Model

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, concat_ws, current_date, lit}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType}

/**
  * @author wuyuantin@easyretailpro.com
  *         2020/06/02  训练逻辑回归LR模型
  *         目前正负样本使用数据是热门前十位商品
  */
object GetLrModel {
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    val daytime= args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("GetModel pro "+brandid)
//    .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //会员基础数据
    val sql_behavior ="select user_id,item_id,behavior_time,ExposeCount,TriggerCount,BrowseCount,CartCount,BuyCount from recommend_pro.base_behavior_count  where brandid = " + brandid +" and dt>regexp_replace(date_sub('"+daytime+"',30), '-', '')"

//    过滤掉没有曝光的商品   触发次数超过曝光的商品
    val behavior=spark.sql(sql_behavior).where($"ExposeCount">0).filter($"TriggerCount"<=$"ExposeCount")
    //获得用户的正样本和负样本  以行为的触发、点击、加购和购买是否产生作为判断
    val behavior_attribute=behavior.withColumn("attr",when($"TriggerCount"+$"BrowseCount"+$"CartCount"+$"BuyCount">0,"1").otherwise("0"))
    val behavior_label = behavior_attribute.select(col("user_id"),col("item_id"),col("attr").alias("label")).dropDuplicates("user_id","item_id")

    //调用函数获取用户特征和商品特征  时间跨度为30天
    val user_id_feture = GetFeture.user_id_feture(spark,behavior_attribute,daytime,30)
    val item_id_feture = GetFeture.item_id_feture(spark,behavior_attribute,daytime,30)
    //把用户特征和商品特征及正负样本做关联
    var train_set = behavior_label.join(user_id_feture,Seq("user_id"),"left")
    train_set = train_set.join(item_id_feture,Seq("item_id"),"left").na.fill(0)
//      for (col<-train_set.columns){
//      train_set = train_set.withColumn(col, when(train_set(col).isNull, 0).otherwise(train_set(col)))}
          //转换特征数据为Double型
    val  train_intput = train_set.select(train_set.drop("user_id","item_id").columns.map(f => col(f).cast(DoubleType)): _*)

    //转换label向量为ml类型的向量
    val stringIndexer = new StringIndexer().
      setInputCol("label").
      setOutputCol("classIndex")//.setHandleInvalid("skip").
      .fit(train_intput)
    val labelTransformed = stringIndexer.transform(train_intput).drop("label")
    //转换训练特征的向量为ml类型向量
    val vectorAssembler = new VectorAssembler().
      setInputCols(train_intput.drop("label").columns).
      setOutputCol("features")

    val lrData = vectorAssembler.transform(labelTransformed).select("features", "classIndex")
    val lrWeight = balanceDataset(lrData)
    val lr = new LogisticRegression()
      .setWeightCol("classWeightCol")
      .setMaxIter(500)
      .setRegParam(0.0)
      .setElasticNetParam(0.0)
      .setFeaturesCol("features")
      .setLabelCol("classIndex")

    //训练模型
    val lrModel= lr.fit(lrWeight)
    //模型相关参数
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //模型摘要
    val trainingSummary = lrModel.binarySummary
    //每次迭代目标值
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))
    //计算模型指标数据
//    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    //模型摘要AUC指标
    val roc = trainingSummary.roc
    //    for(x <- roc){println(x)}
    println("roc.show()")
    roc.show()
    val AUC = trainingSummary.areaUnderROC
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")
    //设置模型阈值 不同的阈值，计算不同的F1，然后通过最大的F1找出并重设模型的最佳阈值。
    val fMeasure = trainingSummary.fMeasureByThreshold
    fMeasure.show()
    // 获得最大的F1值
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    // 找出最大F1值对应的阈值（最佳阈值）
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
    println(bestThreshold)
    // 将模型的Threshold设置为选择出来的最佳分类阈值保存
    lrModel.setThreshold(bestThreshold)

    lrModel.write.overwrite.save(s"/data/lrmodel/"+brandid+"/"+daytime)
    spark.stop()
}
    def balanceDataset(dataset:DataFrame) = {
        // Re-balancing (weighting) of records to be used in the logistic loss objective function
//        dataset.show(1000)
        val numNegatives = dataset.filter(col("classIndex") === 0).count
        val datasetSize = dataset.count
        val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize
        val calculateWeights = udf { d: Double =>
            if (d == 0.0) {1 * balancingRatio}
            else {(1 * (1.0 - balancingRatio))}}
        val weightedDataset = dataset.withColumn("classWeightCol", calculateWeights(dataset("classIndex")))
        weightedDataset
    }
}
