package net.slisenko.examples;

import net.slisenko.AbstractSparkSupportTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;

public class LineLengthTest extends AbstractSparkSupportTestCase implements Serializable {

    @Test
    public void testLineLength() {
        String logFilePath = new File("").getAbsolutePath() + "/src/test/resources/hello-spark.txt";

        // Get text lines from file
        JavaRDD<String> lines = context.textFile(logFilePath);

        // Calculate lengths
        JavaRDD<Integer> linesLength = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        // Sum lengths
        Integer totalLength = linesLength.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
            return integer + integer2;
            }
        });

        System.out.format("Total length: %d \n", totalLength);
    }
}