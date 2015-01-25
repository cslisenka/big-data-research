package net.slisenko.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class HelloStreaming {

    public static void main(String... args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Hello streaming");
        JavaStreamingContext stream = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> strings = stream.socketTextStream("localhost", 9999);

        JavaDStream<String> filteredStrings = strings.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return true;
            }
        });

        filteredStrings.print();

        stream.start();
        stream.awaitTermination();
    }
}
