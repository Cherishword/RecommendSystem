package ezr.bigdata.recommend.test

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, FeatureType}
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

object TestResult {
  case class model_lr(label:java.lang.Double,a:java.lang.Double,b:java.lang.Double)
  def main(args: Array[String]):Unit= {
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("RankItem pro")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //参数准备
    val iteratTree = 10
    val iteratDepth = 10
    val maxAuc = 0.0


    var  train_set = Seq(("1.0", "3.2", "8.0"), ("0.0", "4.2", "15.0"), ("1.0", "2.1", "7.0"), ("0.0", "4.4", "5.0"),
                            ("1.0", "5.1", "12.0"), ("0.0", "7.1", "33.0"), ("1.0", "3.5", "4.0"), ("0.0", "6.8", "9.0"))
                            .toDF("label", "a", "b")
    train_set = train_set.select(train_set.columns.map(f => col(f).cast(DoubleType)): _*)

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
    val gbdtData = vectorAssembler.transform(labelTransformed).select("features", "classIndex")
    val gbdtInput = gbdtData.map(x =>org.apache.spark.mllib.regression.LabeledPoint(x.getAs("classIndex"),
      org.apache.spark.mllib.linalg.Vectors.fromML(x.getAs[org.apache.spark.ml.linalg.SparseVector]("features").toDense)))

    //GBDT参数准备
    val maxDepth = 15
    val numTrees = 10
    val minInstancesPerNode = 2
    // GBDT模型训练
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    boostingStrategy.treeStrategy.minInstancesPerNode = minInstancesPerNode
    boostingStrategy.numIterations = numTrees
    boostingStrategy.treeStrategy.maxDepth = maxDepth
    val gbdtModel = GradientBoostedTrees.train(gbdtInput.rdd, boostingStrategy)

    val treeLeafArray = new Array[Array[Int]](numTrees)
    for (i <- 0.until(numTrees)) {treeLeafArray(i) = getLeafNodes(gbdtModel.trees(i).topNode)}
    val newFeatureDataSet = gbdtInput.map { x =>
      var newFeature = new Array[Double](0)
      for (i <- 0.until(numTrees)) {
        val treePredict = predictModify(gbdtModel.trees(i).topNode, new DenseVector(x.features.toArray))
        //gbdt tree is binary tree
        val treeArray = new Array[Double]((gbdtModel.trees(i).numNodes + 1) / 2)
        treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
        newFeature = newFeature ++ treeArray}
      (x.label, newFeature)}
    val lrInput = newFeatureDataSet.map(x => org.apache.spark.ml.feature.LabeledPoint(x._1, Vectors.dense(x._2))).toDF("classIndex","features")

    //设置逻辑回归参数和输入字段
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("features")
      .setLabelCol("classIndex")

    //训练模型
    val lrModel= lr.fit(lrInput)
    //模型相关参数
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //模型摘要
    val trainingSummary = lrModel.summary
    //每次迭代目标值
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

//    val binarySummary = trainSummary.asInstanceOf[BinaryLogisticRegressionSummary]
//
//    val roc = binarySummary.roc
//    roc.show()



    //10 模型摘要
//        val trainingSummary = lrModel.summary
//    //    //11 每次迭代目标值
//        val objectiveHistory = trainingSummary.objectiveHistory
//        println("objectiveHistory:")
////        objectiveHistory.foreach(loss => println(loss))
//    ////    12 计算模型指标数据
//        val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
//    //    //13 模型摘要AUC指标
//        val roc = binarySummary.roc
//        for(x <- roc){println(x)}
    //    println("roc.show()")
    //    val AUC = binarySummary.areaUnderROC
    //    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")
    //    //15 设置模型阈值
    //    // 不同的阈值，计算不同的F1，然后通过最大的F1找出并重设模型的最佳阈值。
    //    val fMeasure = binarySummary.fMeasureByThreshold
    //    fMeasure.show
    // 获得最大的F1值
    //    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    //    // 找出最大F1值对应的阈值（最佳阈值）
    //    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
    //    // 并将模型的Threshold设置为选择出来的最佳分类阈值
    //    // 保存
    //    lrModel.setThreshold(bestThreshold)
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
    treeLeafNodes
  }
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
