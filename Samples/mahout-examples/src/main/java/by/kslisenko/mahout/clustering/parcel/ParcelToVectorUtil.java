package by.kslisenko.mahout.clustering.parcel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ParcelToVectorUtil {

	private static Vector parcelToVector(Parcel parcel) {
		Vector result = new RandomAccessSparseVector(1);
		double[] vector = new double[1];
		vector[0] = parcel.getWeightInGrams();
		result.assign(vector);
		return new NamedVector(result, parcel.toString());
	}
	
	public static List<Vector> parcelsToVectors(Parcel[] apples) {
		List<Vector> results = new ArrayList<Vector>();
		for (Parcel apple : apples) {
			results.add(parcelToVector(apple));
		}
		return results;
	}
	
	public static void writeVectorsToFile(List<Vector> vectors, Path path, FileSystem fs, Configuration conf) throws IOException {
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : vectors) {
			vec.set(point);
			writer.append(new LongWritable(recNum++), vec);
		}
		writer.close();
	}	
}