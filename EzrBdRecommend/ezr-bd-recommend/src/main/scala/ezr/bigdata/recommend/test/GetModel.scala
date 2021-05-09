package ezr.bigdata.recommend.test

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, current_date, lit}
import org.apache.spark.sql.types.{DateType, DoubleType}

/**
  * @author wuyuantin@easyretailpro.com
  *         2019/12/18  训练逻辑回归LR模型
  *         目前正负样本使用数据是热门前十位商品
  */

object GetModel {
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("GetModel pro"+brandid)
      //.master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //  获取用户行为数据  商品详情数据  热门商品
    val sql_buy ="SELECT orders_dt1.BuyerId vipid, 4 activetype, prod.ItemNo, orders_dt1.daystime "+
      "FROM (SELECT orders.BrandId, orders.BuyerId, DATE_FORMAT(orders.CreateDate,'yyyy-MM-dd') daystime, dtl.ItemId "+
      "FROM tayouhaya_mall_tmp.mall_sales_order orders LEFT JOIN tayouhaya_mall_tmp.mall_sales_order_dtl dtl ON (orders.Id = dtl.OrderId AND orders.BrandId = dtl.BrandId) "+
      "WHERE orders.BrandId = "+ brandid+" and orders.CreateDate > DATE_sub(NOW(),100)) orders_dt1 "+
      "LEFT JOIN tayouhaya_mall_tmp.mall_prd_prod prod ON (orders_dt1.ItemId = prod.Id AND orders_dt1.BrandId = prod.BrandId) WHERE prod.ItemNo > ''"
    val sql_cart = "select vipid,1 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime from tayouhaya_youshu_tmp.add_to_cart where brandid = " + brandid +" and dt>20191107"
    val sql_click = "SELECT vipid,3 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime  FROM tayouhaya_youshu_tmp.browse_sku_page where brandid = " + brandid +" and dt>20191107 "

    val sql_p = "select itemno as item_id,name as item_name,saleprice from tayouhaya_mall_tmp.mall_prd_prod"
    val sql_h = "select item_id,item_hot from recommend_pro"+brandid+".recall_hot_result order by item_hot desc limit 10"
    //转换获得的时间类型数据
    val behavior = spark.sql(sql_buy).union(spark.sql(sql_cart)).union(spark.sql(sql_click)).withColumnRenamed("vipid","user_id")
      .withColumnRenamed("itemno","item_idI").withColumnRenamed("activetype","behavior")
      .withColumn("daystime",col("daystime").cast(DateType))
    val product = spark.sql(sql_p)
    val hot = spark.sql(sql_h)
    //用户行为数据和商品数据做关联
    val user_item = behavior.join(product,product("item_id") === behavior("item_idI"),"inner")
            .drop("item_idI")//.na.replace("behavior" ::Nil, Map("3" -> "4","4"->"10"))
    //获得用户的正样本...  训练数据的点击、加购和购买
    val data_train = user_item.dropDuplicates("user_id","item_id").select("user_id","item_id")
        .withColumn("label" ,lit(1))
    //组合用户id和商品id作为唯一id  方便后面造负样本数据(剔除前十热门商品有行为的数据)
    val data_train_ui = data_train.select(concat_ws("",$"user_id", $"item_id").as("ui"),col("label"))
    //   选择用行为的用户和最热门的商品前十名  作为负样本
    val data_negative = data_train.select("user_id").dropDuplicates("user_id").join(hot.select("item_id"))
    //  组合负样本的user_id和item_id为唯一id
    val data_negative_ui = data_negative.select(col("user_id"), col("item_id"),
                                        concat_ws("",$"user_id", $"item_id").as("ui"))
    //剔除热门商品用户有行为的数据
    val data_negative_label = data_negative_ui.filter(!col("ui").isin(data_train_ui.select("ui").collect().map(_(0)).toList:_*))
                                   .select(col("user_id"),col("item_id"),lit(0))
    //正负样本合并
    val label = data_train.union(data_negative_label)
    //调用函数获取用户特征和商品特征
    val slot = 15
//    val end_time = new DateTime("2019-11-09").toString("yyyy-MM-dd")
    val end_time = current_date()
    val user_id_feture = GetFeture.user_id_feture(user_item,end_time,slot,spark)
    val item_id_feture = GetFeture.item_id_feture(user_item,end_time,slot,spark)
   //把用户特征和商品特征及正负样本做关联
    var train_set = label.join(user_id_feture,label("user_id") === user_id_feture("user_id_behavior"),"left").drop(user_id_feture("user_id_behavior"))
        train_set = train_set.join(item_id_feture,label("item_id") === item_id_feture("item_id_behavior"),"left").drop(item_id_feture("item_id_behavior")).na.fill(0)
    //特征列名重复的重命名  并把null替换为0
//        train_set = GetFeture.RenameDataFrame(train_set,spark)
    //转换特征数据为Double型
     val  train_intput = train_set.select(train_set.drop("user_id","item_id").columns.map(f => col(f).cast(DoubleType)): _*)

    //转换label向量为ml类型的向量
    val stringIndexer = new StringIndexer().
      setInputCol("label").
      setOutputCol("classIndex").
      fit(train_intput)
    val labelTransformed = stringIndexer.transform(train_intput).drop("label")
    //转换训练特征的向量为ml类型向量
    val vectorAssembler = new VectorAssembler().
      setInputCols(train_intput.drop("label").columns).
      setOutputCol("features")
    val lrData = vectorAssembler.transform(labelTransformed).select("features", "classIndex")
    lrData.show()
//    val gbdtInput = gbdtData.map(x =>org.apache.spark.mllib.regression.LabeledPoint(x.getAs("classIndex"),
//              org.apache.spark.mllib.linalg.Vectors.fromML(x.getAs[org.apache.spark.ml.linalg.SparseVector]("features").toDense)))

    //GBDT参数准备
//    val maxDepth = 15
//    val numTrees = 10
//    val minInstancesPerNode = 2
    // GBDT模型训练
//    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
//    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
//    boostingStrategy.treeStrategy.minInstancesPerNode = minInstancesPerNode
//    boostingStrategy.numIterations = numTrees
//    boostingStrategy.treeStrategy.maxDepth = maxDepth
//    val gbdtModel = GradientBoostedTrees.train(gbdtInput.rdd, boostingStrategy)

//    val treeLeafArray = new Array[Array[Int]](numTrees)
//    for (i <- 0.until(numTrees)) {treeLeafArray(i) = getLeafNodes(gbdtModel.trees(i).topNode)}
//    val newFeatureDataSet = gbdtInput.map { x =>
//      var newFeature = new Array[Double](0)
//      for (i <- 0.until(numTrees)) {
//        val treePredict = predictModify(gbdtModel.trees(i).topNode, new DenseVector(x.features.toArray))
        //gbdt tree is binary tree
//        val treeArray = new Array[Double]((gbdtModel.trees(i).numNodes + 1) / 2)
//        treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
//        newFeature = newFeature ++ treeArray}
//      (x.label, newFeature)}
//    val lrInput = newFeatureDataSet.map(x => org.apache.spark.ml.feature.LabeledPoint(x._1, Vectors.dense(x._2))).toDF("classIndex","features")
    //设置逻辑回归参数和输入字段
//    val lr = new LogisticRegression()
//      .setMaxIter(10)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.8)
//      .setFeaturesCol("features")
//      .setLabelCol("classIndex")
//
//    //训练模型
//    val lrModel= lr.fit(lrData)
//    //模型相关参数
//    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//    //模型摘要
//    val trainingSummary = lrModel.summary
//    //每次迭代目标值
//    val objectiveHistory = trainingSummary.objectiveHistory
//    println("objectiveHistory:")
//    objectiveHistory.foreach(loss => println(loss))
////计算模型指标数据
//    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
//    //模型摘要AUC指标
//    val roc = binarySummary.roc
////    for(x <- roc){println(x)}
//    println("roc.show()")
//    roc.show()
//    val AUC = binarySummary.areaUnderROC
//    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")
//    //设置模型阈值 不同的阈值，计算不同的F1，然后通过最大的F1找出并重设模型的最佳阈值。
//    val fMeasure = binarySummary.fMeasureByThreshold
//    fMeasure.show()
//   // 获得最大的F1值
//    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
//    // 找出最大F1值对应的阈值（最佳阈值）
//    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
//    println(bestThreshold)
//    // 将模型的Threshold设置为选择出来的最佳分类阈值保存
//    lrModel.setThreshold(bestThreshold)
//
//    lrModel.write.overwrite.save(s"/data/lrmodel/20191218")
    spark.stop()
  }


  /**
    * gbdt模型解析叶子节点
    */
  def getLeafNodes(node: Node): Array[Int] = {
    var treeLeafNodes = new Array[Int](0)
    if (node.isLeaf) {
      treeLeafNodes = treeLeafNodes.:+(node.id)
    } else {
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.leftNode.get)
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.rightNode.get)
    }
    treeLeafNodes}

  /**
    * 样本数据转换成GBDT叶节点编号的样本
    */
  def predictModify(node:Node,features:DenseVector):Int={
    val split = node.split
    if (node.isLeaf) {
      node.id
    } else {
      if (split.get.featureType == FeatureType.Continuous) {
        if (features(split.get.feature) <= split.get.threshold) {
          //          println("Continuous left node")
          predictModify(node.leftNode.get,features)
        } else {
          // println("Continuous right node")
          predictModify(node.rightNode.get,features)}
      } else {if (split.get.categories.contains(features(split.get.feature))) {
        //  println("Categorical left node")
        predictModify(node.leftNode.get,features)
      } else {
        //  println("Categorical right node")
        predictModify(node.rightNode.get,features)}}}}
}
