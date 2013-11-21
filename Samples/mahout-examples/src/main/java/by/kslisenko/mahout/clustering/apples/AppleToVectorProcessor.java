package by.kslisenko.mahout.clustering.apples;

import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

public class AppleToVectorProcessor {

	public static Vector appleToVector(Apple apple) {
		// TODO color, size
		// TODO normalize values, set weights
		Vector result = new RandomAccessSparseVector(4);
		double[] vector = new double[4];
		vector[0] = apple.getSize().ordinal() * 1.5;
		vector[1] = apple.getColor().compareTo(AppleColor.GREEN);
		vector[2] = apple.getColor().compareTo(AppleColor.RED);
		vector[3] = apple.getColor().compareTo(AppleColor.YELLOW);
//		vector[5] = apple.getWeightInGrams() / 300;
		result.assign(vector);
		return new NamedVector(result, apple.toString());
	}
	
	public static List<Vector> applesToVectors(Apple[] apples) {
		List<Vector> results = new ArrayList<Vector>();
		for (Apple apple : apples) {
			results.add(appleToVector(apple));
		}
		return results;
	}
}