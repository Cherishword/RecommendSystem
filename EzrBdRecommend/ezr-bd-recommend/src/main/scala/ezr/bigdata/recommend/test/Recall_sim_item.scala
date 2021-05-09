package ezr.bigdata.recommend.test

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, regexp_replace, row_number, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Recall_sim_item {
  def main(args: Array[String]): Unit = {
    val BrandId: String = args(0)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Recall_sim_item pro" + BrandId)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //读取商品和行为数据
    val sql_p = "select itemno,name,saleprice from  tayouhaya_mall_tmp.mall_prd_prod where brandid = " + BrandId
    val product = spark.sql(sql_p)
    //替换掉多样屋及【.*?】字符
    val cor = product.select(col("itemno"), regexp_replace(col("name"), "【.*?】|多样屋", "").alias("name"))
    //使用jieba来切词  把字符长度1的词过滤掉
    def jieba_seg(df: DataFrame, colname: String): DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jieba_udf = udf { (sentence: String) =>
        val segV = seg.value
        segV.process(sentence.toString, SegMode.SEARCH).toArray().map(_.asInstanceOf[SegToken].word).filter(_.length > 1).mkString("/")
      }
      df.withColumn("words", jieba_udf(col(colname)))
    }

    //调用jieba的udf  获得切的词
    val df_seg = jieba_seg(cor, "name").select("itemno", "words")
    //由于切的词需要横向循环对比，只能把数据缓存到分区   有对节点有一定风险
    val itemlist = df_seg.collect()
    //商品描述之间两两对半 计算相同的词和不同的词的比值
    val score = new ArrayBuffer[(String, String, String)]()
    for (i <- 0 to itemlist.length - 2) {
      for (j <- i + 1 to itemlist.length - 1) {
        val tags1 = (itemlist(i)(1).toString().split("/").toSet.|(itemlist(j)(1).toString().split("/").toSet)).size //商品i和商品j的标签并集长度
        val tags2 = (itemlist(i)(1).toString().split("/").toSet.&(itemlist(j)(1).toString().split("/").toSet)).size //商品i和商品j的标签的交集长度
        if (tags2 > 0) {
          score += ((itemlist(i)(0).toString(), itemlist(j)(0).toString(), (math.round(tags2.toDouble / tags1 * 100) * 0.001d).toString()))
        }
      }
    }
    //结果重命名
    val result = score.toDF().withColumnRenamed("_1", "ItemI").withColumnRenamed("_2", "ItemJ").withColumnRenamed("_3", "score")
    //  对商品分组，以得分做排序，取前十名
    val result_top = result.withColumn("rank", row_number.over(Window.partitionBy("ItemI").orderBy(col("score").desc)))
      .where($"rank" <= 10) //.drop("rank")

    //导入数据
    spark.sql("truncate table recommend_pro" + BrandId + ".recall_sim_item")
    result_top.write.mode(SaveMode.Overwrite).insertInto("recommend_pro" + BrandId + ".recall_sim_item")
    spark.stop()
  }
}
