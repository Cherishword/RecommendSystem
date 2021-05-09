package ezr.bigdata.recommend.Feature

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * 待完成，聚类结果有异议
  * */

object Feature_cate_price {
  def main(args: Array[String]): Unit = {
    val brandid: String = args(0)
    val daytime= args(1)
    //环境
    val spark = SparkSession
      .builder()
      .appName("feature_cate_price pro " + brandid)
      //      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //    商品价格大于0   最后选出来的必须有购买 取出各级类目
    val sql_prod_cate = "select behavior.item_id item_id,prod.CategoryId CategoryId,behavior.buytotalcount buycount,prod.price itemprice " +
      " from (select item_id,count(buycount) buytotalcount from recommend_pro.base_behavior_count behavior " +
      " where brandid = "+brandid+" and dt>regexp_replace(date_sub('"+daytime+"',100), '-', '') and buycount>0 group by item_id) behavior " +
      " left join (select ItemNo item_id,MasterCategoryId CategoryId,price from pro.rtl_prod where price>0 and dt= regexp_replace('"+ daytime +"', '-', '') and brandidpartition=" +brandid+
      " union all select ItemNo item_id,SecondCategoryId CategoryId,price from pro.rtl_prod where price>0 and dt= regexp_replace('"+ daytime +"', '-', '') and brandidpartition=" +brandid+
      " union all select ItemNo item_id,ThirdCategoryId  CategoryId,price from pro.rtl_prod where price>0 and and dt= regexp_replace('"+ daytime +"', '-', '') and brandidpartition=" +brandid+") prod"+
      " on behavior.item_id = prod.item_id"
    val prod_cate = spark.sql(sql_prod_cate)
    //    取类目下数量大于100的商品
    val cate_filter=prod_cate.groupBy("CategoryId").agg(count($"CategoryId")).filter($"count(CategoryId)">100)
      .select($"CategoryId").collect()
    if (cate_filter.length >0){
      //清表
      val sql1 =  "alter table recommend_pro.feature_cate_price drop IF EXISTS partition(brandid = "+brandid +")"
      spark.sql(sql1)

      //    循环取出类目聚类
      for (cateid <- cate_filter){
        val user_filter = prod_cate.where($"CategoryId" === cateid(0))
        val  train_intput = user_filter.select($"Categoryid",$"item_id",$"itemprice".cast(DoubleType),$"buycount".cast(DoubleType))
        val parsedData = train_intput.rdd.map(s => Vectors.dense(s.getDouble(2),s.getDouble(3))).cache()
        val clusters = KMeans.train(parsedData, 3, 30)
        val t = udf { (x1: Double, x2: Double) => clusters.predict(Vectors.dense(x1, x2)) }
        //      获取聚类后每个类目的类别
        val result = train_intput.select($"Categoryid",$"item_id",$"itemprice",$"buycount",t(col("itemprice"), col("buycount")))
          .withColumnRenamed("UDF(itemprice, buycount)","cluster")
        //      取出没个分类下最高三个金额和最低三个金额，然后求出平均值
        val clustermin0 = result.where($"cluster"=== 0).orderBy("itemprice").limit(3)
          .groupBy("cluster").agg(avg("itemprice"))
        val clustermax0 = result.where($"cluster"=== 0).orderBy(desc("itemprice")).limit(3)
          .groupBy("cluster").agg(avg("itemprice"))
        val clustermin1 = result.where($"cluster"=== 1).orderBy("itemprice").limit(3)
          .groupBy("cluster").agg(avg("itemprice"))
        val clustermax1 = result.where($"cluster"=== 1).orderBy(desc("itemprice")).limit(3)
          .groupBy("cluster").agg(avg("itemprice"))
        val clustermin2 = result.where($"cluster"=== 2).orderBy("itemprice").limit(3)
          .groupBy("cluster").agg(avg("itemprice"))
        val clustermax2 = result.where($"cluster"=== 2).orderBy(desc("itemprice")).limit(3)
          .groupBy("cluster").agg(avg("itemprice"))
        clustermin0.show()
        clustermax0.show()
        clustermin1.show()
        clustermax1.show()
        clustermin2.show()
        clustermax2.show()
        //      表关联   金额排序
        val price_range =clustermin0.union(clustermax0).union(clustermin1).union(clustermax1).union(clustermin2).union(clustermax2)
          .orderBy("avg(itemprice)").withColumn("rank", row_number.over(Window.orderBy(col("avg(itemprice)"))))
        //价格带1
        val price1 = price_range.where($"rank"===2 || $"rank"===3)
          .select(mean($"avg(itemprice)")).select(round($"avg(avg(itemprice))",2).as("price1"),lit(cateid(0)).as("CategoryId"))
        //      价格带2
        val price2 = price_range.where($"rank"===4  || $"rank"===5)
          .agg(mean($"avg(itemprice)")).select(round($"avg(avg(itemprice))",2).as("price2"),lit(cateid(0)).as("CategoryId"))
        val price = price1.join(price2,Seq("CategoryId"),"full")
        //写入hive
        price.repartition(2).persist().createOrReplaceTempView("price_range")
        val sql2 = "insert into table recommend_pro.feature_cate_price partition(brandid ="+brandid+") select CategoryId,price1,price2 from price_range"
        spark.sql(sql2)
      }
    }
  }
}