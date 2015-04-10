package net.slisenko.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kslisenko on 1/25/15.
 */
public class HelloSpark {

    public static void main(String... args) {
        String logFilePath = "/home/kslisenko/Environment/spark-1.2.0-bin-hadoop2.4/README.md";
        SparkConf conf = new SparkConf().setAppName("Hello spark").setMaster("local[2]");
        // Context tells Spark how to access cluster
        JavaSparkContext context = new JavaSparkContext(conf);

        // Get collection
        JavaRDD<String> initialData = context.textFile(logFilePath).cache();
        JavaRDD<String> allWords = initialData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        List<Tuple2<String, Integer>> wordCount = allWords.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).collect();

        // Word count
        System.out.println("Word counts: " + wordCount.size());
        for (Tuple2<String, Integer> value : wordCount) {
            System.out.println(value);
        }

        System.out.println("Lines count = " + initialData.count());
        System.out.println("All words count = " + allWords.count());
//        initialData.map(new Function<String, Integer>() {
//            @Override
//            public Integer call(String s) throws Exception {
//                return 1;
//            }
//        });

//        JavaRDD<String> containsA = initialData.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return s.contains("a");
//            }
//        });
//
//        JavaRDD<String> containsB = initialData.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return s.contains("b");
//            }
//        });
//
//        System.out.println("Contains a length: " + containsA.count());
//        System.out.println("Contains a first: " + containsA.first());
//        System.out.println("Contains b length: " + containsB.count());
//        System.out.println("Contains b first: " + containsB.first());
//
//        try {
//            Thread.sleep(10000000);
//        } catch (InterruptedException e) {
//        }
    }
}