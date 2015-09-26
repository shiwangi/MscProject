/**
 * Created by shiwangi on 4/9/15.
 */

import java.io.{InputStreamReader, BufferedReader}
import java.util

import me.lemire.integercompression._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.MutableList

class Posting(docId: Int, var termFrequency: Int) extends Serializable {
  var _id: Int = docId
  var _frequency: Int = termFrequency

}

object MySimpleApp {
  def reduce(a: Posting, b: Posting): Posting = {
    if (a._id == b._id) (new Posting(a._id, a._frequency + b._frequency))
    else a


  }

  def compress(postings: mutable.MutableList[Int]): Array[Int] = {
    val codec: IntegratedIntegerCODEC = new
        IntegratedComposition(
          new IntegratedBinaryPacking(),
          new IntegratedVariableByte());
    // output vector should be large enough...
    val data: Array[Int] = postings.toArray;
    //Array.fill(256){0}
    var compressed: Array[Int] = Array.fill(data.length + 1024) {
      0
    };
    // compressed might not be large enough in some cases
    // if you get java.lang.ArrayIndexOutOfBoundsException, try
    // allocating more memory

    /**
     *
     * compressing
     *
     */
    val inputoffset: IntWrapper = new IntWrapper(0);
    val outputoffset: IntWrapper = new IntWrapper(0);
    codec.compress(data, inputoffset, postings.length, compressed, outputoffset);
    // got it!
    // inputoffset should be at data.length but outputoffset tells
    // us where we are...
    System.out.println("compressed from " + (data.length * 4.0) /(1.024) + "B to " + (outputoffset.intValue() * 4.0) / 1.024 + "B");
    // we can repack the data: (optional)
    compressed = util.Arrays.copyOf(compressed, outputoffset.intValue());
    compressed
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Inverted Index")
    val sc = new SparkContext(conf)
    println("Enter number if files you want to deal with");
    val input: InputStreamReader   = new InputStreamReader(System.in);
   val br: BufferedReader  = new BufferedReader(input);
    val nFiles:Int = Integer.parseInt(br.readLine());
    var wordsMappedPosting: RDD[(String, Posting)] = sc.emptyRDD;
    for (i <- 1 to nFiles){
      val logFile = "/home/shiwangi/spark-1.1.0/README.md" // Read the fileName instead
      val logData = sc.textFile(logFile, 2).cache()
      val docId = i;

      val words = logData.flatMap(line => line.split(" "))

      val wordsMappedToOne: RDD[(String, Posting)] = words.map(word => (word, new Posting(docId, 1)))
      wordsMappedPosting = wordsMappedPosting.++(wordsMappedToOne);
    }





    val map = scala.collection.mutable.Map.empty[String, Array[Int]]
    val listWord = wordsMappedPosting.groupByKey();

    for ((s, post) <- listWord) {
      val postingMap = post.map(p => (p._id, p._frequency));
      val groupedPosting = postingMap.groupBy(_._1);
      var postings: MutableList[Int] = MutableList();
      for ((id, freq) <- groupedPosting) {
        val x = freq.foldLeft(0)((r, c) => r + c._2)
        println(s + " -> (" + id + " , " + x + ")")
        postings += (id);
        postings += x;
      }
      val compressedPostings: Array[Int] = compress(postings)
      map(s) = compressedPostings
    }

  }
}
