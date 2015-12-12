import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.twitter.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.*;
import java.util.Arrays;
import scala.Tuple2;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;


public class Tutorial {
  public static JavaSparkContext sparkcontext;
  public static void main(String[] args) throws Exception {
    // Location of the Spark directory 
    String sparkHome = "/root/spark";
    
    // URL of the Spark cluster
    String sparkUrl = "local[6]";

    // Location of the required JAR files 
    String jarFile = "target/scala-2.10/tutorial_2.10-0.1-SNAPSHOT.jar";

    // HDFS directory for checkpointing
    String checkpointDir = "checkpoint/";

    // Create JavaStreamingContext object
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterHelloWorldExample");
    sparkcontext = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sparkcontext, new Duration(60000));

    // Twitter Authorization keys
    System.setProperty("twitter4j.oauth.consumerKey", "TDWR8MUScoUEDOftRtppV4urA");
    System.setProperty("twitter4j.oauth.consumerSecret", "ROilWoXSNZbGzYLU5uc1j4WDBHaJe3MWmdWSlE5hdVPzZHDuG7");
    System.setProperty("twitter4j.oauth.accessToken", "2980937292-mC14gSGpew0acNig7G1dgvIpkWVdUjGUjIiDD5o");
    System.setProperty("twitter4j.oauth.accessTokenSecret", "sZyWOchLo8eh6V730Kf3kZ54tg63fc2JgGTU8uN7HZB4X");

    // apply filters
    String[] filters = new String[] {"justin"};

    // create Twitter stream
    JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, filters);

    // get output text of all tweets
    JavaDStream<String> statuses = twitterStream.map(
            new Function<Status, String>() {
                public String call(Status status) { return status.getText(); }
            }
    );

    // split tweet stream into word stream (split a tweet into words)
    JavaDStream<String> words = statuses.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterable<String> call(String in) {
          return Arrays.asList(in.split(" "));
        }
      }
    );

    // apply filter and get hashtags only
    JavaDStream<String> hashTags = words.filter(
      new Function<String, Boolean>() {
        public Boolean call(String word) { return word.startsWith("#"); }
      }
    );
     
    //hashTags.print();     // to print intermediate result

    /*
    *   1. Remove Hash from the words
    *   2. Lower case all words 
    *   3. Convert Hashtags into Tuples of (<Hashtag>, 1)  format
    */
    JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String in) {
          return new Tuple2<String, Integer>(in.substring(1).toLowerCase(), 1);
        }
      }
    );

    /*
    *   1. Reduce by Key
    *   2. Perform sliding window operation
    */
    JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer i1, Integer i2) { return i1 + i2; }
      },
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer i1, Integer i2) { return i1 - i2; }
      },
      new Duration(60 * 5 * 1000),    /* Window Length */
      new Duration(60 * 5 * 1000)     /* Sliding Interval */
    );

    //    counts.print();     // check intermediate results

    /*
    *   1. Map function to swap the Tuple (Tuple<word, count> to Tuple<count, word>)
    */ 
    JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(
      new PairFunction<Tuple2<String, Integer>, Integer, String>() {
        public Tuple2<Integer, String> call(Tuple2<String, Integer> in) {
          return in.swap();
        }
      }
    );

    // Apply transformation to sort by hashtags/key (by count)
    JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(
      new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
        public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
          return in.sortByKey(false);
        }
      }
    );


    // fetch the top 25 hashtags    
    sortedCounts.foreach(
      new Function<JavaPairRDD<Integer, String>, Void> () {
        public Void call(JavaPairRDD<Integer, String> rdd) {
          //rdd.take(25).toString();

          String out = "\nTop 25 hashtags:\n";
          for (Tuple2<Integer, String> t: rdd.take(25)) {
            out = out + t.toString() + "\n";
           }
           System.out.println(out);       // print on the console
          // store top 25 hashtags on the file
          List<String> temp = new ArrayList<String>();
          temp.add(out);
          JavaRDD<String> distData = sparkcontext.parallelize(temp);
          distData.saveAsTextFile("./output/yenwo-output.txt");
           return null;
        }
      }
    );


    jssc.checkpoint(checkpointDir);
  	jssc.start();    	
    jssc.awaitTermination();
  }
}
