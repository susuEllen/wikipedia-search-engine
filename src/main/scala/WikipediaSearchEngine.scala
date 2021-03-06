/**
 * Created by ellenwong on 11/20/16.
 */



//import org.apache.mahout.text.wikipedia

//import com.cloudera.datascience.common.XmlInputFormat
//import org.apache.hadoop.conf.Configuration

//import edu.umd.cloud9.collection.wikipedia.language

import java.io.{FileWriter, BufferedWriter, File}

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.storage.StorageLevel

//import com.databricks.spark.xml
import edu.umd.cloud9.collection.XMLInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level,Logger}
import org.apache.hadoop.conf.Configuration



object WikipediaSearchEngine {
  def main(args: Array[String]): Unit = {

    println("\nHello! Lets build a wikipedia search engine\n")
    val searchEngine = new CorpusSearchEngine
    searchEngine.run()
  }
}

import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage

class CorpusSearchEngine extends Serializable{
  // state stored in here
  //val path = "/Users/ellenwong/Development/data/wikidump/enwiki-latest-pages-articles-multistream.xml"
  val localDirectory = new java.io.File(".").getCanonicalPath
  val path = localDirectory +  "/enwiki-latest-pages-articles-multistream.xml"
  val rdd = localDirectory + "/rdd/"
  val resultFile = localDirectory + "/output.txt"

  def run() = {
    // (1) load data
    turnOffSparkJunk()
    val conf = new SparkConf().setAppName("WikipediaSearchEngine").setMaster("local")
    @transient val hconf = new Configuration()
    hconf.set(XMLInputFormat.START_TAG_KEY, "<page>")
    hconf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    val sc = new SparkContext(conf)
    val kvs = sc.newAPIHadoopFile(path, classOf[XMLInputFormat], classOf[LongWritable], classOf[Text], conf = hconf)
    val rawXmls: RDD[String] = kvs.map(_._2.toString)
    //rawXmls.cache()

    def wikiXmlToPlainText(xml: String): Option[(String, String)] = {
      val page = new EnglishWikipediaPage()
      WikipediaPage.readPage(page, xml)

      if (page.isEmpty) None
      else Some((page.getTitle, page.getContent))
    }
    val plainText: RDD[(String, String)] = rawXmls.flatMap(wikiXmlToPlainText)
    plainText.saveAsObjectFile(rdd)
    //plainText.saveAsTextFile(rdd)

    println(s"plainText length: ${plainText.count()}")
//    plainText.persist(StorageLevel.DISK_ONLY)
//    plainText.checkpoint()
    val corpus: Array[(String, String)] = plainText.collect()

    //TODO: write doc to disk??

    val outputFile = new File(resultFile)
    val bw = new BufferedWriter(new FileWriter(outputFile))
    bw.write(s"first document: ${corpus.head}")
    bw.close()

    sc.stop()
  }

//  def turnBigFileToDocs() = {
//
//  }
//
//  def parseDataFromXMLToPlainText() = {
//
//  }


  def turnOffSparkJunk() = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }



  //Logic:
  // (1) Load data
  // (2) (a) Split up large xml doc into documents,
  //     (b) Parse data from xml into plain text with Cloud9
  // (3) Turn Plain text into bag of terms. [using Stanford NLP library]
  //     (a) filter stop words,
  //     (b) lemmatize words of similar meaning into same term (or bag)
  // (4) Compute TF-IDFs from RDDs of terms to document Matrix
  // (5) Use SVD to reduce the size of Matrix
  // (6) Calculate Term-Term Relevance
  // (7) Calculate Document-Document Relevance
  // (8) Calculate Term-Document Relevance
  // (9) Do some searches from results above

  /*
  * Notes:
  *  (1) everything inside map needs to be serializable,
  *  e.g use anonymous function instead of instance methods, or make sure class method is from a serializable class
  * (2) collect before accessing an element in RDD
  * */

}
