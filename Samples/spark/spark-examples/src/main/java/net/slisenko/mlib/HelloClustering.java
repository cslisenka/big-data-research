package net.slisenko.mlib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HelloClustering implements Serializable {

    public static void main(String... ars) {
        SparkConf conf = new SparkConf().setAppName("Hello clustering").setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<Double> values = context.parallelize(Arrays.asList(1.0, 1.1, 1.3, 2.0, 2.1, 2.3, 5.0, 5.1, 5.3, 15.0));

        JavaRDD<Vector> vectorValues = values.map(new Function<Double, Vector>() {
            @Override
            public Vector call(Double aDouble) throws Exception {
                return Vectors.dense(aDouble);
            }
        });

        int numberOfClusters = 4;
        int maxIterations = 20;
        int runs = 1; //?
        final KMeansModel model = KMeans.train(vectorValues.rdd(), numberOfClusters, maxIterations, runs, KMeans.K_MEANS_PARALLEL());

        JavaPairRDD<Integer, Vector> clusterToPoint = vectorValues.mapToPair(new PairFunction<Vector, Integer, Vector>() {
            @Override
            public Tuple2<Integer, Vector> call(Vector vector) throws Exception {
                return new Tuple2(model.predict(vector), vector);
            }
        });

        for (Vector center : model.clusterCenters()) {
            System.out.format("Cluster %s \n", center);
        }

        System.out.println("Clusters to points: " + clusterToPoint.collect());
    }
}