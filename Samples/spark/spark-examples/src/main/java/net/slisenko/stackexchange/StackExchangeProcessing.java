
package net.slisenko.stackexchange;

import net.slisenko.stackexchange.model.StackExchangePost;
import net.slisenko.stackexchange.util.StackExchangePostParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.streaming.StreamInputFormat;
import org.apache.hadoop.streaming.StreamXmlRecordReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.util.Iterator;

public class StackExchangeProcessing {

    public static void main(String... args) {
        SparkConf conf = new SparkConf().setAppName("Hello spark").setMaster("local[3]");
        JavaSparkContext context = new JavaSparkContext(conf);

        String xmlUrl = new File("").getAbsolutePath() + "/src/test/resources/posts-small.xml";
        String resultUrl = new File("").getAbsolutePath() + "/target/stackoverflow-parsing-results";
        JobConf jobConf = new JobConf();
        jobConf.set("stream.recordreader.class", StreamXmlRecordReader.class.getName());
        jobConf.set("stream.recordreader.begin", "<row Id=");
        jobConf.set("stream.recordreader.end", "/>");
        FileInputFormat.setInputPaths(jobConf, new Path(xmlUrl));

        // Read rows from xml
        JavaPairRDD<Text, Text> posts = context.hadoopRDD(jobConf, StreamInputFormat.class, Text.class, Text.class);
        JavaRDD<String> postsString = posts.map(new Function<Tuple2<Text, Text>, String>() {
            @Override
            public String call(Tuple2<Text, Text> post) throws Exception {
                return post._1().toString();
            }
        });

        // Parse rows, extract ID, title, text
        JavaRDD<StackExchangePost> parsedPosts = postsString.mapPartitions(new FlatMapFunction<Iterator<String>, StackExchangePost>() {
            @Override
            public Iterable<StackExchangePost> call(final Iterator<String> stringIterator) throws Exception {
                return new StackExchangePostParser(stringIterator);
            }
        });

        parsedPosts.saveAsTextFile(resultUrl);

        System.out.println(parsedPosts.count());
        StackExchangePost first = parsedPosts.first();
        System.out.println("ID " + first.getId());
        System.out.println("TITLE " + first.getTitle());
        System.out.println("TEXT " + first.getText());
    }
}