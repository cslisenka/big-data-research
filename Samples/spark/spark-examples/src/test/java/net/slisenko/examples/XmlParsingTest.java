package net.slisenko.examples;

import net.slisenko.AbstractSparkSupportTestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.streaming.StreamInputFormat;
import org.apache.hadoop.streaming.StreamXmlRecordReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;

/**
 * Example how to use hadoop input format to read XML files
 */
public class XmlParsingTest extends AbstractSparkSupportTestCase implements Serializable {

    @Test
    public void test() {
        String xmlUrl = new File("").getAbsolutePath() + "/src/test/resources/posts-small.xml";
        JobConf jobConf = new JobConf();
        jobConf.set("stream.recordreader.class", StreamXmlRecordReader.class.getName());
        jobConf.set("stream.recordreader.begin", "<row Id=");
        jobConf.set("stream.recordreader.end", "/>");
        FileInputFormat.setInputPaths(jobConf, new Path(xmlUrl));

        JavaPairRDD<Text, Text> posts = context.hadoopRDD(jobConf, StreamInputFormat.class, Text.class, Text.class);

        JavaRDD<String> postsString = posts.map(new Function<Tuple2<Text, Text>, String>() {
            @Override
            public String call(Tuple2<Text, Text> post) throws Exception {
                return post._1().toString();
            }
        });

        System.out.println(postsString.count());
        System.out.println(postsString.first());
    }
}