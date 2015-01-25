package net.slisenko;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;

/**
 * Manages spark context, base for unit-tests with spark
 */
public abstract class AbstractSparkSupportTestCase {

    protected SparkConf conf;
    protected JavaSparkContext context;

    @Before
    public void setUp() {
        conf = new SparkConf().setAppName("Spark test").setMaster("local");
        context = new JavaSparkContext(conf);
    }

    @After
    public void tearDown() {
        context.stop();
    }
}
