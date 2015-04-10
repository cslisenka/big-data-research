package net.slisenko.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class HelloSparkSQL {

    public static void main(String... args) {
        String logFilePath = "/home/kslisenko/IdeaProjects/big-data-research/Samples/spark/spark-examples/src/main/resources/links.json";
        SparkConf conf = new SparkConf().setAppName("Hello spark").setMaster("local[2]");
        // Context tells Spark how to access cluster
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaSQLContext sqlContext = new JavaSQLContext(context);

        JavaRDD<Friendship> friendshipRdd = context.textFile(logFilePath).map(new Function<String, Friendship>() {
            @Override
            public Friendship call(String s) throws Exception {
                String clean = s.replace("[", "").replace("]", "").replace("\"", "");
                String[] pair = clean.split(", ");

                Friendship fr = new Friendship();
                fr.setFriend1(pair[0]);
                fr.setFriend2(pair[1]);
                return fr;
            }
        });

        JavaSchemaRDD schemaFriendship = sqlContext.applySchema(friendshipRdd, Friendship.class);
        schemaFriendship.registerTempTable("friends");

        // SQL
//        JavaSchemaRDD tomFriendships = sqlContext.sql("SELECT * FROM friends WHERE friend1='Tom' or friend2='Tom'");
//        System.out.println(tomFriendships.collect());

        JavaSchemaRDD friendship = sqlContext.sql("SELECT DISTINCT t1.friend1, t1.friend2 FROM friends t1 INNER JOIN friends t2 ON (t1.friend1 = t2.friend2 AND t1.friend2 = t2.friend1)");
        System.out.println(friendship.collect());
    }
}