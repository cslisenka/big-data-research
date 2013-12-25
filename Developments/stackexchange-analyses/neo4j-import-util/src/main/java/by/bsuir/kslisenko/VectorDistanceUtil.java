package by.bsuir.kslisenko;

import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.Vector;

public class VectorDistanceUtil {

	public static double getCosineDistance(Vector v1, Vector v2) {
		CosineDistanceMeasure dist = new CosineDistanceMeasure();
		return dist.distance(v1, v2);
	}
}