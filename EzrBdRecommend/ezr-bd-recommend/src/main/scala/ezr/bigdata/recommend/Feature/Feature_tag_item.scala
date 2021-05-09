package ezr.bigdata.recommend.Feature

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Feature_tag_item {
  def main(args: Array[String]): Unit = {
//    val shardingGrpid = args(0)
//    val shardingid = args(0)
    val brandid: String = args(0)
    val dt =args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled","true")
      .appName("feature_tag_item pro " + brandid)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    //只要商品或商品的类目有变动，就重新跑一边数据
    val sql_prod_refresh = "select DATE_FORMAT(lastmodifieddate,'yyyyMMdd') from pro.rtl_prod prod where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition=" + brandid +" order by lastmodifieddate desc limit 1"
    val sql_category_refresh = "select DATE_FORMAT(lastmodifieddate,'yyyyMMdd') from pro.rtl_prod_category where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition=" + brandid +" order by lastmodifieddate desc limit 1"
    val dt_time1 =spark.sql(sql_prod_refresh).rdd.collect().toString
    val dt_time2 =spark.sql(sql_category_refresh).rdd.collect().toString
    if (dt_time1 >= dt.replace("-","") || dt_time2 >= dt.replace("-","")) {
      tag_item(spark,brandid,dt)
    }
  }

/**
  *商品三级类目并行，把商品三级类目、价格、商品名作为商品的标签计算商品之间的相似度
  * */
  def tag_item(spark:SparkSession,brandid:String,dt:String): Unit ={
    import spark.implicits._
    //读取商品类目和名称
    val sql_rtl_prod = "select ItemNo item_id,MasterCategoryId,SecondCategoryId,ThirdCategoryId,name ItemName,Price from pro.rtl_prod prod where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition=" + brandid
    val sql_prod_category = "select id,name from pro.rtl_prod_category where dt= regexp_replace('"+ dt +"', '-', '') and brandidpartition=" + brandid
    //    val sql_mall_prod = "select itemno item_id,name ItemName from pro.mall_prd_prod_temp where brandid = " + brandid
    //    val mall_prod = spark.sql(sql_mall_prod)
    val rtl_prod  = spark.sql(sql_rtl_prod)
    val prod_category = spark.sql(sql_prod_category)
    //    val prod = rtl_prod.join(prod_category,rtl_prod("MasterCategoryId")===prod_category("id") ,"left").withColumnRenamed("name","MasterCategoryName")
    //      .join(prod_category,rtl_prod("SecondCategoryId")===prod_category("id") ,"left").withColumnRenamed("name","SecondCategoryName")
    //      .join(prod_category,rtl_prod("ThirdCategoryId")===prod_category("id") ,"left").withColumnRenamed("name","ThirdCategoryName")
    //      .select("item_id","MasterCategoryName","SecondCategoryName","ThirdCategoryName","Price","ItemName")
    /** 1 把商品类目的id和名称放到map里 map(id,name) */
    val myMap = prod_category.rdd.map(r=>(r.getAs[Int]("id"),r.getAs[String]("name")))
      .collect() // 标签应该最多上万条完全可以使用.collect()不会内存溢出
      .toMap
    /** 2 把商品类目的map注册成广播变量 */
    val broadcastMap = spark.sparkContext.broadcast(myMap).value
    /**3 注册一个我们自己的 UDF 函数（取名字叫：getCategory）传入商品标签Id使用广播变量拿出标签名称，没有匹配上的返回null */
    spark.udf.register("getCategory",(id:Int)=>{if(broadcastMap.contains(id)){broadcastMap.get(id)}else{null}})
    /** 4 使用sparkSQL 的 selectExpr 方法调用 自己定义的UDF 函数 getCategory(id) 同时 使用as 完成重命名*/
    val prod = rtl_prod.selectExpr("item_id", "getCategory(MasterCategoryId) as MasterCategoryName", "getCategory(SecondCategoryId) as SecondCategoryName", "getCategory(ThirdCategoryId) as ThirdCategoryName", "Price", "ItemName")
    //   商品标签一
    val prod_tag1=prod.select("item_id","MasterCategoryName").filter("MasterCategoryName is not null")
      .withColumnRenamed("MasterCategoryName","tag").dropDuplicates("item_id","tag").withColumn("tag_type",lit(1))
    //   商品标签二
    val prod_tag2=prod.select("item_id","SecondCategoryName").filter("SecondCategoryName is not null")
      .withColumnRenamed("SecondCategoryName","tag").dropDuplicates("item_id","tag").withColumn("tag_type",lit(2))
    //   商品标签三
    val prod_tag3=prod.select("item_id","ThirdCategoryName").filter("SecondCategoryName is not null")
      .withColumnRenamed("ThirdCategoryName","tag").dropDuplicates("item_id","tag").withColumn("tag_type",lit(3))
    //    //   商品标签四 价格
    val prod_tag4=prod.select("item_id","Price").filter(col("Price")=!="0.00")
      .withColumnRenamed("Price","tag").dropDuplicates("item_id","tag").withColumn("tag_type",lit(4))
    //    //   商品标签五
    //    //替换掉森马及【.*?】字符
    val ItemName_filter = prod.select(col("item_id"), regexp_replace(col("ItemName"), "【.*?】|森马", "").as("ItemName"))
    val cut_word = jieba_seg(spark,ItemName_filter,"ItemName").filter(length($"words")>0)
    //    拿出商品名多个标签
    val item_word: RDD[Row] = cut_word.rdd.mapPartitions(p=>{
      val lstBf:ListBuffer[Row] = ListBuffer()
      p.foreach(r=>{
        val ItemNo: String = r.getAs[String]("item_id")
        val wordsArr: Array[String] = r.getAs[String]("words").split("/")
        for(c <- wordsArr){
          lstBf+=Row(ItemNo,c)
        }
      })
      lstBf.toIterator})
    val prod_tag5 = item_word.map({case Row(val1: String,val2: String) => (val1,val2)}).toDF("item_id","tag").dropDuplicates("item_id","tag")
      .withColumn("tag_type",lit(5))
    val item_tag = prod_tag1.union(prod_tag2).union(prod_tag3).union(prod_tag4).union(prod_tag5)

    item_tag.repartition(2).persist().createOrReplaceTempView("item_tag")
    val sql2 = "insert overwrite table recommend_pro.feature_tag_item partition(brandid ="+brandid+") select item_id,tag_type,tag from item_tag"
    spark.sql(sql2)
      }

  def jieba_seg(spark: SparkSession,df: DataFrame, colname: String): DataFrame = {
    val segmenter = new JiebaSegmenter()
    val seg = spark.sparkContext.broadcast(segmenter)
    val jieba_udf = udf { (sentence: String) =>
      val segV = seg.value
      segV.process(sentence.toString, SegMode.SEARCH).toArray().map(_.asInstanceOf[SegToken].word).filter(_.length > 1).mkString("/")}
    df.withColumn("words", jieba_udf(col(colname)))}
}
