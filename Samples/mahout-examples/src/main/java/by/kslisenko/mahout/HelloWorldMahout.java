package by.kslisenko.mahout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Example of simple k-means clustering
 * 
 * @author kslisenko
 */
public class HelloWorldMahout {
	
	private static final String TESTDATA_DIR = "target/testdata";
	private static final String TESTDATA_POINTS_DIR = TESTDATA_DIR + "/points";
	private static final String INPUT_FILE_PATH = TESTDATA_DIR + "/points/file1";
	private static final String INITIAL_CLUSTERS_DIR = TESTDATA_DIR + "/clusters";
	private static final String INITIAL_CLUSTERS_FILE_PATH = TESTDATA_DIR + "/clusters/part-00000";
	
	// Dots to be clustered
	public static final double[][] points = { 
		{ 1, 1 }, { 2, 1 }, { 1, 2 },
		{ 2, 2 }, { 3, 3 }, { 8, 8 }, 
		{ 9, 8 }, { 8, 9 }, { 9, 9 }
	};

	public static void writePointsToFile(List<Vector> points, String fileName, FileSystem fs, Configuration conf) throws IOException {
		Path path = new Path(fileName);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			vec.set(point);
			writer.append(new LongWritable(recNum++), vec);
		}
		writer.close();
	}

	public static List<Vector> getPoints(double[][] raw) {
		List<Vector> points = new ArrayList<Vector>();
		for (int i = 0; i < raw.length; i++) {
			double[] fr = raw[i];
			Vector vec = new RandomAccessSparseVector(fr.length);
			vec.assign(fr);
			points.add(vec);
		}
		return points;
	}

	public static void main(String args[]) throws Exception {
		// Number of clusters we want to have
		int k = 2;

		List<Vector> vectors = getPoints(points);

		FileUtils.deleteDirectory(new File(TESTDATA_DIR));
		new File(TESTDATA_POINTS_DIR).mkdirs();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// Write input data to file for mahout
		writePointsToFile(vectors, INPUT_FILE_PATH, fs, conf);

		// Write to file initial cluster centroids (can be any dots)
		Path path = new Path(INITIAL_CLUSTERS_FILE_PATH);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Cluster.class);

		// In our example we write first to dots
		for (int i = 0; i < k; i++) {
			Vector vec = vectors.get(i);
			// USe Euclidean measure
			Cluster cluster = new Cluster(vec, i, new EuclideanDistanceMeasure());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer.close();

		// Run k-means clustering algorithm
		KMeansDriver.run(conf, new Path(INPUT_FILE_PATH), new Path(INITIAL_CLUSTERS_DIR), new Path("out"), new EuclideanDistanceMeasure(), 0.001, 10, true, false);

		// Read output file and out results
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("out/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000"),conf);

		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		while (reader.next(key, value)) {
			System.out.println(value.toString() + " belongs to cluster " + key.toString());
		}
		reader.close();
	}
}