package net.slisenko;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by kslisenko on 1/25/15.
 */
public class HelloSpark {

    public static void main(String... args) {
        String logFilePath = "/home/kslisenko/Environment/spark-1.2.0/README.md";
        SparkConf conf = new SparkConf()
                .setAppName("Hello spark") // Application name on UI
                .setMaster("local"); // URL of spark/hadoop cluster, or "local"
        // Context tells Spark how to access cluster
        JavaSparkContext context = new JavaSparkContext(conf);

        // Get collection
        JavaRDD<String> initialData = context.textFile(logFilePath).cache();

        JavaRDD<String> containsA = initialData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("a");
            }
        });

        JavaRDD<String> containsB = initialData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("b");
            }
        });

        System.out.println("Contains a length: " + containsA.count());
        System.out.println("Contains a first: " + containsA.first());
        System.out.println("Contains b length: " + containsB.count());
        System.out.println("Contains b first: " + containsB.first());
    }
}