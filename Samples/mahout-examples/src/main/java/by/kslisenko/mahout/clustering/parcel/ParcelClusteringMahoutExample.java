package by.kslisenko.mahout.clustering.parcel;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.Canopy;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

/**
 * Example of simple k-means clustering
 * 
 * @author kslisenko
 */
public class ParcelClusteringMahoutExample {

    private static final Path testDataPath = new Path("target/testdata");
    private static final Path inputPointsPath = new Path(testDataPath, "inputPoints");
    private static final Path initialClustersPath = new Path(testDataPath, "initialClusters");
    private static final Path outputPath = new Path(testDataPath, "out");

    private static final Path outputClustersPath = new Path(outputPath, "clusters-3-final/part-r-00000");
    private static final Path outputPointsPath = new Path(outputPath, "clusteredPoints/part-m-00000");

    public static final Parcel[] parcels = {
            new Parcel(2500), new Parcel(1500), new Parcel(400), new Parcel(100),
            new Parcel(150), new Parcel(300), new Parcel(2000), new Parcel(250),
            new Parcel(150), new Parcel(1000)
    };

    public static void main(String args[]) throws Exception {
        int k = 3;
        List<Vector> parcelVectors = ParcelToVectorUtil.parcelsToVectors(parcels);
        Configuration conf = new Configuration();

        HadoopUtil.delete(conf, testDataPath);
        new File(testDataPath.getName()).mkdirs();

        FileSystem fs = FileSystem.get(conf);
        ParcelToVectorUtil.writeVectorsToFile(parcelVectors, inputPointsPath, fs, conf);

        RandomSeedGenerator.buildRandom(conf, inputPointsPath, initialClustersPath, k, new EuclideanDistanceMeasure());
        KMeansDriver.run(inputPointsPath, initialClustersPath, outputPath, 0.1, 6, true, 0.0, false);

        readClusters(outputClustersPath);
        readPoints(conf, fs, outputPointsPath);
    }

    private static void readPoints(Configuration conf, FileSystem fs, Path clusteredPointsPath) throws IOException {
        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, clusteredPointsPath, conf);
        while (reader.next(key, value)) {
            System.out.println("Cluster Id: " + key.toString() + " value: " + value);
        }
        reader.close();
    }

    private static void readClusters(Path clustersPath) throws IOException {
        Configuration conf = new Configuration();
        for (ClusterWritable value : new SequenceFileDirValueIterable<ClusterWritable>(clustersPath, PathType.LIST,
                PathFilters.logsCRCFilter(), conf)) {
            Cluster cluster = value.getValue();
            System.out.println("Cluster Id: " + cluster.getId() +
                    " center: " + cluster.getCenter() +
                    " points: " + cluster.getNumObservations() +
                    " radius: " + cluster.getRadius());
        }
    }
}