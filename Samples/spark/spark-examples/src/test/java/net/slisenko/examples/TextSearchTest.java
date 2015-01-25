package net.slisenko.examples;

import net.slisenko.AbstractSparkSupportTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;

public class TextSearchTest extends AbstractSparkSupportTestCase implements Serializable{

    @Test
    public void test() {
        String filePath = new File("").getAbsolutePath() + "/src/test/resources/hello-spark.txt";

        JavaRDD<String> fileStrings = context.textFile(filePath);
        JavaRDD<String> searchResults = fileStrings.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.toLowerCase().contains("spark");
            }
        });

        System.out.format("Search count: %d \n", searchResults.count());
        System.out.println("Strings: " + searchResults.collect());
    }
}
