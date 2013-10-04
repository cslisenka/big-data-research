package by.kslisenko.mahout.apples;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class ApplesClusteringMahoutExample {
	
	private static final String TESTDATA_DIR = "target/testdata";
	private static final String TESTDATA_POINTS_DIR = TESTDATA_DIR + "/points";
	private static final String INPUT_FILE_PATH = TESTDATA_DIR + "/points/file1";
	private static final String INITIAL_CLUSTERS_DIR = TESTDATA_DIR + "/clusters";
	private static final String INITIAL_CLUSTERS_FILE_PATH = TESTDATA_DIR + "/clusters/part-00000";
	
	public static final Apple[] apples = {
		new Apple(100, AppleColor.RED, AppleSize.SMALL),
		new Apple(120, AppleColor.GREEN, AppleSize.LARGE),
		new Apple(500, AppleColor.YELLOW, AppleSize.MEDIUM),
		new Apple(300, AppleColor.RED, AppleSize.SMALL),
		new Apple(250, AppleColor.GREEN, AppleSize.SMALL),
		new Apple(115, AppleColor.YELLOW, AppleSize.LARGE),
		new Apple(2800, AppleColor.RED, AppleSize.SMALL),
		new Apple(540, AppleColor.YELLOW, AppleSize.LARGE),
		new Apple(200, AppleColor.GREEN, AppleSize.MEDIUM),
		new Apple(120, AppleColor.RED, AppleSize.SMALL),
		new Apple(140, AppleColor.RED, AppleSize.LARGE),
		new Apple(500, AppleColor.YELLOW, AppleSize.MEDIUM),
		new Apple(101, AppleColor.RED, AppleSize.SMALL),
		new Apple(130, AppleColor.RED, AppleSize.LARGE),
		new Apple(520, AppleColor.GREEN, AppleSize.MEDIUM),
		new Apple(156, AppleColor.YELLOW, AppleSize.SMALL),
		new Apple(325, AppleColor.RED, AppleSize.LARGE),
		new Apple(731, AppleColor.GREEN, AppleSize.LARGE),
		new Apple(530, AppleColor.YELLOW, AppleSize.SMALL),
		new Apple(601, AppleColor.RED, AppleSize.SMALL),
	};

	public static void writeVectorsToFile(List<Vector> appleectors, String fileName, FileSystem fs, Configuration conf) throws IOException {
		Path path = new Path(fileName);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : appleectors) {
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
		int k = 3;

		List<Vector> appleVectors = AppleToVectorProcessor.applesToVectors(apples);

		FileUtils.deleteDirectory(new File(TESTDATA_DIR));
		new File(TESTDATA_POINTS_DIR).mkdirs();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// Write input data to file for mahout
		writeVectorsToFile(appleVectors, INPUT_FILE_PATH, fs, conf);

		// Write to file initial cluster centroids (can be any dots)
		Path path = new Path(INITIAL_CLUSTERS_FILE_PATH);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Cluster.class);

		// In our example we write first to dots
		for (int i = 0; i < k; i++) {
			Vector vec = appleVectors.get(i);
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
		Map<String, List<String>> results = new HashMap<String, List<String>>();
		while (reader.next(key, value)) {
			if (!results.containsKey(key.toString())) {
				results.put(key.toString(), new ArrayList<String>());
			}
			
			results.get(key.toString()).add(value.toString());
		}
		reader.close();
		
		// Out results by clusters
		for (String cluster : results.keySet()) {
			System.out.println("Cluster " + cluster);
			for (String apple : results.get(cluster)) {
				System.out.println(apple);
			}
		}
	}
}