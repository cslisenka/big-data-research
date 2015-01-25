package net.slisenko.examples;

import net.slisenko.AbstractSparkSupportTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;

public class WordCountTest extends AbstractSparkSupportTestCase implements Serializable {

    @Test
    public void test() {
        String filePath = new File("").getAbsolutePath() + "/src/test/resources/hello-spark.txt";
        JavaRDD<String> strings = context.textFile(filePath);

        JavaRDD<Integer> wordCountEachString = strings.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.split(" ").length;
            }
        });

       Integer wordCount = wordCountEachString.reduce(new Function2<Integer, Integer, Integer>() {
           @Override
           public Integer call(Integer integer, Integer integer2) throws Exception {
               return integer + integer2;
           }
       });

        System.out.format("Word count: %d \n", wordCount);
    }
}