//package ezr.bigdata.recommend.test
//
//import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
//import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//
//import scala.collection.mutable.ListBuffer
//
///**
//  * @author wuyuantin@easyretailpro.com
//  *         2019/12/22  生成用户对商品标签偏好来推荐商品
//  */
//object Recall_tag_result {
//  def main(args: Array[String]):Unit= {
//    val brandid: String = args(0)
//  //环境
//  val spark = SparkSession
//    .builder()
//    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    .config("spark.sql.crossJoin.enabled","true")
//    .appName("Recall_tag_result pro"+brandid)
////    .master("local[*]")
//    .enableHiveSupport()
//    .getOrCreate()
//  import spark.implicits._
//
//  //读取商品和用户行为数据
//  val sql_p = "select itemno,name as item_name,saleprice from tayouhaya_mall_tmp.mall_prd_prod where brandid = " + brandid
//    val sql_buy ="SELECT orders_dt1.BuyerId vipid, 4 activetype, prod.ItemNo, orders_dt1.daystime "+
//      "FROM (SELECT orders.BrandId, orders.BuyerId, DATE_FORMAT(orders.CreateDate,'yyyy-MM-dd') daystime, dtl.ItemId "+
//      "FROM tayouhaya_mall_tmp.mall_sales_order orders LEFT JOIN tayouhaya_mall_tmp.mall_sales_order_dtl dtl ON (orders.Id = dtl.OrderId AND orders.BrandId = dtl.BrandId) "+
//      "WHERE orders.BrandId = "+ brandid+" and orders.CreateDate > DATE_sub(NOW(),100)) orders_dt1 "+
//      "LEFT JOIN tayouhaya_mall_tmp.mall_prd_prod prod ON (orders_dt1.ItemId = prod.Id AND orders_dt1.BrandId = prod.BrandId) WHERE prod.ItemNo > ''"
//    val sql_cart = "select vipid,1 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime from tayouhaya_youshu_tmp.add_to_cart where brandid = " + brandid +" and dt>20191107"
//    val sql_click = "SELECT vipid,3 activetype,spu_id,to_date(from_unixtime(cast(time/1000 as int))) daystime  FROM tayouhaya_youshu_tmp.browse_sku_page where brandid = " + brandid +" and dt>20191107 "
//    val behavior = spark.sql(sql_buy).union(spark.sql(sql_cart)).union(spark.sql(sql_click)).withColumnRenamed("vipid","user_id")
//          .withColumnRenamed("itemno","item_id").withColumnRenamed("activetype","behavior")
//  val product = spark.sql(sql_p)
//  //用户行为和商品详情作关联
//    val user_behavior = behavior.join(product,product("itemno") === behavior("item_id"),"inner")
//                              .select("user_id","item_id","behavior", "daystime", "item_name", "saleprice")
//                              //数据去重
//                              //.dropDuplicates("user_id","item_id","daystime")
//                              //每个行为分数  点击一分  收藏两分  加购四分  购买十分
//                             .na.replace("behavior" ::Nil, Map("1" ->"1" ,"2"->"2","3"->"4" ,"4"->"10"))
//                             //.withColumn("diff", datediff(lit(new DateTime("2019-12-09").toString("yyyy-MM-dd")),col("daystime")))
//                             //距今时间差
//                              .withColumn("diff", datediff(lit(current_date()),col("daystime")))
//                              //增加分值时间衰减因素
//                              .withColumn("behavior",col("behavior")/col("diff")).drop("diff","daystime")
//
//    //替换掉多样屋及【.*?】字符
//    val product_word = product.select(col("itemno"),regexp_replace(col("item_name"),"【.*?】|多样屋","").alias("item_name"))
//    //使用jieba来切词  把字符长度1的词过滤掉
//    def jieba_seg(df:DataFrame,colname:String): DataFrame ={
//      val segmenter = new JiebaSegmenter()
//      val seg = spark.sparkContext.broadcast(segmenter)
//      val jieba_udf = udf{(sentence:String)=>val segV = seg.value
//        segV.process(sentence.toString,SegMode.SEARCH).toArray().map(_.asInstanceOf[SegToken].word).filter(_.length>1).mkString("/")
//      }
//      df.withColumn("words",jieba_udf(col(colname)))}
//    //调用jieba的udf  获得商品切的词后具体标签
//    val item_tags = jieba_seg(product_word,"item_name").select("itemno","words")
//    //商品和每一个标签相关度
//    val itemsTagsCor = item_tags.rdd.mapPartitions(p=> {
//      val listXs: ListBuffer[Tuple3[String,String,String]] = ListBuffer()
//      p.foreach(r => {val A = r.getAs[String]("itemno")
//        val B = r.getAs[String]("words")
//        for(b <- B.split("/")){listXs.append((A,b,"1"))} })
//      listXs.toIterator}).toDF("itemno","word","cor")
//    //用户行为和标签做关联
//    val user_behavior_tag = user_behavior.join(itemsTagsCor,user_behavior("item_id")===itemsTagsCor("itemno"),"left")
//    //用户对标签打标签次数
//    val userTagNum = user_behavior_tag.groupBy("user_id","word").agg(sum("cor"))
//      //某个标签被所有用户打标签次数
//    val tagUserNum = user_behavior_tag.groupBy("word").agg(sum("cor"))
//     //用户对商品的行为总分
//    val userRate = user_behavior.groupBy("user_id","item_id").agg(sum("behavior"))
//    //用户对商品相关的标签分数及次数
//    var userTagPre = userRate.join(itemsTagsCor,userRate("item_id") === itemsTagsCor("itemno"),"left").drop("itemno")
//      .groupBy("user_id","word").agg(sum(col("cor")*col("sum(behavior)")).as("rate_ui"),count("cor"))
//    //用户对标签的依赖程度TF
//    var tf_ut = userTagNum.groupBy("user_id").agg(count("word")).withColumnRenamed("user_id","user_id1")
//    tf_ut = userTagNum.join(tf_ut,userTagNum("user_id") === tf_ut("user_id1"),"left").drop("user_id1")
//      .withColumn("tf_ut",col("sum(cor)")/col("count(word)")).select("user_id","word","tf_ut")
//       .withColumnRenamed("user_id","user_id1").withColumnRenamed("word","word1")
//
//    //优化用户对标签的依赖程度  逆文本频率IDF  增加log(总人数/标签被打标总人数)
//    val idf_ut = tagUserNum.withColumn("idf_ut",log(lit(tagUserNum.count())/col("sum(cor)")+1)).select("word","idf_ut")
//                          .withColumnRenamed("word","word1")
//
////    用户对标签的兴趣度
//    userTagPre = userTagPre.join(tf_ut,(userTagPre("user_id")=== tf_ut("user_id1"))&&(userTagPre("word")=== tf_ut("word1")) ,"left").drop("user_id1","word1")
//    userTagPre = userTagPre.join(idf_ut,userTagPre("word")=== idf_ut("word1") ,"left").drop("word1")
//    userTagPre =userTagPre.withColumn("pre",col("rate_ui")/col("count(cor)")*col("idf_ut")*col("tf_ut"))
//                              .select("user_id","word","pre")
//
////     //用户对商品偏好笛卡尔积太大   为了切分用户  取出所有有行为的用户
////    val user_all = userTagPre.select("user_id").dropDuplicates("user_id").map(_(0).toString).collect().toList
////    //取出所有的商品
////    val item_all = user_behavior.withColumn("ui",concat_ws("",col("user_id"),col("item_id")))
////                                .select("ui").collect().map(_(0)).toList
////    //取出所有用户行为的商品  方便后续过滤
////    val user_ui = user_behavior.dropDuplicates("user_id","item_id")
////                        .withColumn("ui",concat_ws("",col("user_id"),col("item_id"))).select("ui")
////    //创建空的DF   方便和得到的最后结果进行拼接
////    val schema = StructType(Seq(StructField("user_id", StringType, true), StructField("itemno", StringType, true),
////                                StructField("ui", StringType, true), StructField("item_pre", StringType, true),
////                                StructField("rank", StringType, true)))
////    var userPreResult = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
////
////    //计算用户对商品喜爱程度  切分用户 分成10份
////    for (x<- user_all.grouped(user_all.size/5)){
////      val user_cut = userTagPre.filter(col("user_id").isin(x:_*))
////      // 商品标签相关度表和用户标签偏好表作笛卡尔积
////       var userItemPre = itemsTagsCor.join(user_cut)
////      //计算用户标签和相关度乘积  用户和商品标ui
////      userItemPre = userItemPre.withColumn("item_pre",col("pre")*col("cor"))
////                              .withColumn("ui",concat_ws("",col("user_id"),col("itemno")))
////                              .select("user_id","itemno","ui","item_pre")
////      //计算用户对商品中标签偏好值的和
////      userItemPre = userItemPre.groupBy("user_id","itemno","ui").agg(sum("item_pre").as("item_pre"))
////      //过滤已有行为的商品
////    userItemPre = userItemPre.filter(!col("ui").isin(user_ui.collect().map(_(0)).toList:_*))
////      //取出排序前十名
////      userItemPre = userItemPre.withColumn("rank",row_number.over(Window.partitionBy("user_id")
////        .orderBy(col("item_pre").desc))).where($"rank" <= 10).orderBy(col("user_id").desc)
////      //和空的DF拼接
////      userPreResult = userPreResult.union(userItemPre)
////    }
//    // 用户id和商品id组合  方便过滤有行为的商品
//   val user_ui = user_behavior.dropDuplicates("user_id","item_id")
//                 .withColumn("ui",concat_ws("",col("user_id"),col("item_id"))).select("ui")
//
//    // 商品标签相关度表和用户标签偏好表作笛卡尔积
//    var userItemPre = userTagPre.join(itemsTagsCor,userTagPre("word")=== itemsTagsCor("word"),"left")
//                     .withColumn("item_pre",col("pre")*col("cor"))
//                     .withColumn("ui",concat_ws("",col("user_id"),col("itemno")))
//                     .select("user_id","itemno","ui","item_pre")
//
//    //计算用户对商品中标签偏好值的和
//    userItemPre = userItemPre.groupBy("user_id","itemno","ui").agg(sum("item_pre").as("item_pre"))
//    //过滤已有行为的商品
//    userItemPre = userItemPre.filter(!col("ui").isin(user_ui.collect().map(_(0)).toList:_*))
//    //取出排序前十名
//    userItemPre = userItemPre.withColumn("rank",row_number.over(Window.partitionBy("user_id")
//      .orderBy(col("item_pre").desc))).where($"rank" <= 10).orderBy(col("user_id").desc)
//    //导入数据
//    spark.sql("truncate table recommend_pro"+brandid+".recall_tag_result")
//    userItemPre.select(col("user_id"),col("itemno").as("item_id"),col("item_pre").as("tag_score"))
//                .write.mode(SaveMode.Overwrite).insertInto("recommend_pro"+brandid+".recall_tag_result")
//    spark.stop()
//}
//}
