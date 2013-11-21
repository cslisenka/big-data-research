package by.kslisenko.mahout.clustering.inmemory;

import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.clustering.display.DisplayKMeans;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansClusterer;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;

import by.kslisenko.mahout.RandomPointsUtil;

/**
 * This sample generates 2-dimensional random points and runs in-memory
 * k-means clustering algorithm. Here we put initial numbers of clusters.
 * 
 * @author cloudera
 */
public class InMemoryKMeansClustering {

	public static void main(String[] args) throws Exception {
		// Input data
		List<Vector> sampleInputData = new ArrayList<Vector>();
		RandomPointsUtil.generateSamples(sampleInputData, 400, 1, 1, 3);
		RandomPointsUtil.generateSamples(sampleInputData, 300, 1, 0, 0.5);
		RandomPointsUtil.generateSamples(sampleInputData, 300, 0, 2, 0.1);
		
		int clustersCount = 3;
		
		// Stop clustering if at some step cluster places changed less than this value
		double treshold = 0.01;
		
		// Choose first three random clusters. This clusters are start point for the algorithm.
		// This clusters created by selected random points.
		List<Cluster> initialRandomClusters = new ArrayList<Cluster>();
		List<Vector> randomPoints = RandomPointsUtil.chooseRandomPoints(sampleInputData, clustersCount);
		int clusterId = 0;
		for (Vector v : randomPoints) {
			initialRandomClusters.add(new Cluster(v, clusterId++, new EuclideanDistanceMeasure()));
		}
		
		// Run clustering algorithm using Euclidean distance measure
		List<List<Cluster>> finalClusters = KMeansClusterer.clusterPoints(sampleInputData, initialRandomClusters, new EuclideanDistanceMeasure(), clustersCount, treshold);
		
		for (Cluster cluster : finalClusters.get(finalClusters.size() - 1)) {
			System.out.println("Cluster id: " + cluster.getId() + " center: " + cluster.getCenter().asFormatString());
		}
	}
}