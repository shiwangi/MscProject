/**
 * Created by shiwangi on 4/9/15.
 */

import java.io._
import java.text.DecimalFormat
import java.util

import me.lemire.integercompression._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.MutableList

class Posting(docId: Int, var termFrequency: Int) extends Serializable  {
  var _id: Int = docId
  var _frequency: Int = termFrequency

}
object PostingOrdering extends Ordering[Posting] {
  def compare(a:Posting, b:Posting) = a._id compare b._id
}

object MySimpleApp {


  val conf = new SparkConf().setAppName("Inverted Index")
  val sc = new SparkContext(conf)
  /**
   * Compresses the list of Postings
   *
   * @param postings
   * @return
   */
  def compress(postings: mutable.MutableList[Int]): List[Int] = {
    val codec: IntegratedIntegerCODEC = new
        IntegratedComposition(
          new IntegratedBinaryPacking(),
          new IntegratedVariableByte());
    // output vector should be large enough...
    val data: Array[Int] = postings.toArray;
    var compressed: Array[Int] = Array.fill(data.length + 1024) {
      0
    };

    /**
     *
     * compressing
     *
     */

    val inputoffset: IntWrapper = new IntWrapper(0);
    val outputoffset: IntWrapper = new IntWrapper(0);
    codec.compress(data, inputoffset, postings.length, compressed, outputoffset);

//    System.out.println("compressed from " + (data.length * 4.0) / (1.024) + "B to " + (outputoffset.intValue() * 4.0) / 1.024 + "B");
    // we can repack the data: (optional)
    compressed = util.Arrays.copyOf(compressed, outputoffset.intValue());
    compressed.toList
  }

  var y:List[(String,List[Int])] = List[(String,List[Int])]();

  def reduce(listWord: RDD[(String,Iterable[Posting])]): List[(String,List[Int])] = {


    for ((word, post) <- listWord) {
      val postingMap = post.map(p => (p._id, p._frequency));
      val groupedPosting = postingMap.groupBy(_._1);
      var postings: MutableList[Int] = MutableList();
      for ((id, freq) <- groupedPosting) {
        val x = freq.foldLeft(0)((r, c) => r + c._2)
        postings += (id);
        postings += x;
      }

      val compressedPostings: List[Int] = compress(postings);
      y = (word,compressedPostings)::y;

    }
    y
  }

  def main(args: Array[String]) {

    val filePath = "/home/shiwangi/Downloads/wtx001/B"

    println("Enter number if files you want to deal with");

    val input: InputStreamReader = new InputStreamReader(System.in);
    val br: BufferedReader = new BufferedReader(input);
    val nFiles: Int = Integer.parseInt(br.readLine());

    //The RDD for all words and Postings
    var wordsMappedPosting: RDD[(String, Posting)] = sc.emptyRDD;
    var sol = "";
    /**
     * Populating the RDD
     */
    System.out.println("Enter file path")
    //val logFile = br.readLine()
    //val logFile = "/home/shiwangi/inverted"
    for (i <- 1 to nFiles) {
      val df:DecimalFormat = new DecimalFormat("00");
      val docNum = df.format(i);
      val logFile = filePath + String.valueOf(docNum);
      val linesList = sc.textFile(logFile, 2).cache()
      val specialCharFreeLinesList = linesList.map(line => line.replaceAll("<.*>|[^A-Za-z0-9 ]",""))
      val logData = specialCharFreeLinesList.map(line => line.toLowerCase())
      val docId = i;
      val words = logData.flatMap(line => line.split(" "))
      val wordsMappedToOne: RDD[(String, Posting)] = words.map(word => (word, new Posting(docId, 1)))
      wordsMappedPosting = wordsMappedPosting.++(wordsMappedToOne);
    }
    wordsMappedPosting.collect.toSeq.sortBy(_._2)(PostingOrdering)
//    for(a <- wordsMappedPosting){
//      print(a._1)
//      val p:Posting = a._2
//      println(" "+p._id+" "+p._frequency)
//
//    }
    val map = scala.collection.mutable.Map.empty[String, Array[Int]]

    /**
     * Here I'm performing a transformation on a RDD ~~Returns a pointer to a RDD
     * So essentially listWord Should be an RDD
     */
    val listWord: RDD[(String,Iterable[Posting])] = wordsMappedPosting.groupByKey();

//    for(a <- listWord){
//      print(a._1)
//      val postings = a._2
//      for (p <- postings) {
//        println(p._id + " " + p._frequency)
//      }
//
//    }
    /**
     * TODO : Will all these operations be performed on clusters? Which of the operations are actions and which ones Transformation?
     */
    val file = new File("/home/shiwangi/postingsOutput.txt")

    val compressedPostings: List[(String,List[Int])]  = reduce(listWord);
    val x:RDD[(String,List[Int])] = sc.parallelize(y,1);
    x.saveAsTextFile("/home/shiwangi/PostOut")
    sc.stop();
  }
}
