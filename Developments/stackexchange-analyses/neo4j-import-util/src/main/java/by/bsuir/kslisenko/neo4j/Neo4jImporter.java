package by.bsuir.kslisenko.neo4j;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.vectors.VectorHelper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.rest.graphdb.RestGraphDatabase;

import bbuzz2011.stackoverflow.join.ClusteredDocument;
import by.bsuir.kslisenko.util.ReaderHandler;
import by.bsuir.kslisenko.util.SequenceFileReaderUtil;

/**
 * Imports clusters and points to neo4j
 * @author kslisenko
 */
public class Neo4jImporter {

//	private static final String SERVER = "http://50.16.193.54:7474/db/data";
	private static final String SERVER = "http://localhost:7474/db/data";
	private static final int BATCH_SIZE = 25000; // Objects imported per transaction
	private GraphDatabaseService graphDb;
	private String serverUri;
	private String pathToClusters;
	private String pathToDictionary;
	private String pathToPoints;
	
	private String[] dictionary;
	
	private Index<Node> clusterIndex;
	private Index<Node> pointsIndex;
	private Index<Node> experimentIndex;
	private Node experimentNode;
	
	private Transaction currentTx;
	private int itemsInTransaction = 0;
	
	public Neo4jImporter(String serverUri, String pathToClusters, String pathToPoints, String pathToDictionary) {
		this.serverUri = serverUri;
		this.pathToClusters = pathToClusters;
		this.pathToDictionary = pathToDictionary;
		this.pathToPoints = pathToPoints;
	}
	
	public void addExperimentRootNode(String dataset, String algorithmUsed, String otherDescription) {
		experimentIndex = graphDb.index().forNodes("experiments");
		currentTx = graphDb.beginTx();
		
		experimentNode = graphDb.createNode();
		long id = System.currentTimeMillis();
		experimentNode.setProperty("id", id);
		experimentNode.setProperty("dataset", dataset);
		experimentNode.setProperty("algorithmUsed", algorithmUsed);
		experimentNode.setProperty("otherDescription", otherDescription);
		experimentNode.setProperty("importDate", new Date());
		experimentIndex.add(experimentNode, "id", id);
		
		Node rootNode = graphDb.getNodeById(0);
		rootNode.createRelationshipTo(experimentNode, RelTypes.EXPERIMENT);
		
		currentTx.success();
		currentTx.finish();
	}
	
	public void doImport(String dataset, String algorithmUsed, String otherDescription) throws IOException {
		long startTime = System.currentTimeMillis();
		graphDb = new RestGraphDatabase(serverUri);
		
		Configuration conf = new Configuration();
		dictionary = VectorHelper.loadTermDictionary(conf, pathToDictionary);
		
		addExperimentRootNode(dataset, algorithmUsed, otherDescription); 
		
		// Import clusters
		addClusters(conf);
		
		// Import points
		addPoints(conf);
		System.out.println("Time: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds");
	}

	private void addPoints(Configuration conf) throws IOException {
		pointsIndex = graphDb.index().forNodes("points");
		
		// TODO use VectorHelper.vectorToSortedString(vector, dictionary)
		ReaderHandler<LongWritable, ClusteredDocument> handler = new ReaderHandler<LongWritable, ClusteredDocument>() {
			@Override
			public void before() throws IOException {
				currentTx = graphDb.beginTx();
			}

			@Override
			public void read(LongWritable key, ClusteredDocument value, PrintStream out) throws IOException {
				addPoint(key.toString(), value.getClusterId().toString(), value.getDocumentTitle().toString(), value.getDocumentContent().toString());
				
				itemsInTransaction++;
				if (itemsInTransaction >= BATCH_SIZE) {
					// Finish previous transaction and start new
					currentTx.success();
					currentTx.finish();
					currentTx = graphDb.beginTx();
					itemsInTransaction = 0;
					System.out.println("Commit transaction with " + itemsInTransaction + " nodes");
				}			
			}

			@Override
			public void after() throws IOException {
				currentTx.success();
				currentTx.finish();
				System.out.println("Commit transaction with " + itemsInTransaction + " nodes");
				itemsInTransaction = 0;				
			}
		};
		
		currentTx = graphDb.beginTx();
		
		SequenceFileReaderUtil.readPartFilesInDir(pathToPoints, 10000000, conf, handler);
	}
	
	private void addPoint(String id, String clusterId, String title, String content) {
		Node pointNode = graphDb.createNode();
		pointNode.setProperty("id", id);
		pointNode.setProperty("title", title);
		pointNode.setProperty("content", content);
		pointsIndex.add(pointNode, "id", id);
		
		Node clusterNode = clusterIndex.get("id", clusterId).getSingle();
		clusterNode.createRelationshipTo(pointNode, RelTypes.CONTAINS);
		// Property can be similarity measure
	}

	private void addClusters(Configuration conf) throws IOException {
		// Create index for clusters
		clusterIndex = graphDb.index().forNodes("clusters");
		
		// Add root cluster
		Transaction tx = graphDb.beginTx();
		final Node rootNode;
		try {
			rootNode = graphDb.createNode();
			rootNode.setProperty("name", "root");
			tx.success();
		} finally {
			tx.finish();
		}
		
		ReaderHandler<IntWritable, ClusterWritable> handler = new ReaderHandler<IntWritable, ClusterWritable>() {
			@Override
			public void before() throws IOException {
				currentTx = graphDb.beginTx();
			}

			@Override
			public void read(IntWritable key, ClusterWritable value, PrintStream out) throws IOException {
				String name = getClusterName(value, dictionary);
				addCluster(value.getValue().getId() + "", name, value.getValue().getNumObservations(), rootNode);
				itemsInTransaction++;
				if (itemsInTransaction >= BATCH_SIZE) {
					// Finish previous transaction and start new
					currentTx.success();
					currentTx.finish();
					currentTx = graphDb.beginTx();
					itemsInTransaction = 0;
					System.out.println("Commit transaction with " + itemsInTransaction + " nodes");
				}
			}

			@Override
			public void after() throws IOException {
				currentTx.success();
				currentTx.finish();
				System.out.println("Commit transaction with " + itemsInTransaction + " nodes");
				itemsInTransaction = 0;
			}
		};
		
		SequenceFileReaderUtil.readPartFilesInDir(pathToClusters, 1000000, conf, handler);
	}
	
	private void addCluster(String id, String name, long numPoints, Node rootNode) {
		Node clusterNode = graphDb.createNode();
		clusterNode.setProperty("id", id);
		clusterNode.setProperty("name", name);
		clusterNode.setProperty("numPoints", numPoints);
		clusterIndex.add(clusterNode, "id", id);
		experimentNode.createRelationshipTo(clusterNode, RelTypes.CONTAINS);
	}
	
	public static void main(String[] args) throws IOException {
		final String BASE = "../stackexchange-analyses-hadoop-mahout/target/stackoverflow-output-base/";
		Neo4jImporter importer = new Neo4jImporter(SERVER, 
				BASE + "kmeans/clusters-1-final",
				BASE + "clusteredPosts_kmeans",
				BASE + "sparse/dictionary.file-*");
		importer.doImport("stackoverflow.com small", "k-means clustering (30 clusters), mahout 0.8", "small stackoverflow subset from Frank Sholten demo");
		
		importer = new Neo4jImporter(SERVER, 
				BASE + "fuzzy-kmeans/clusters-1-final",
				BASE + "clusteredPosts_fuzzy-kmeans",
				BASE + "sparse/dictionary.file-*");
		importer.doImport("stackoverflow.com small", "fyzzy k-means clustering (30 clusters), mahout 0.8", "small stackoverflow subset from Frank Sholten demo");		
	}
	
	private static class TermIndexWeight {
		private final int index;
		private final double weight;

		TermIndexWeight(int index, double weight) {
			this.index = index;
			this.weight = weight;
		}
	}
	
	protected String getClusterName(ClusterWritable cluster, String[] dictionary) {
		List<TermIndexWeight> vectorTerms = new ArrayList<TermIndexWeight>();

		for (Vector.Element elt : cluster.getValue().getCenter().all()) {
			vectorTerms.add(new TermIndexWeight(elt.index(), elt.get()));
		}

		// Sort results in reverse order (ie weight in descending order)
		Collections.sort(vectorTerms, new Comparator<TermIndexWeight>() {
			@Override
			public int compare(TermIndexWeight one, TermIndexWeight two) {
				return Double.compare(two.weight, one.weight);
			}
		});

		int index = vectorTerms.get(0).index;
		String clusterName = dictionary[index];

		if (clusterName.equals("")) {
			clusterName = "Unknown";
		}

		return clusterName;
	}
	
	private static enum RelTypes implements RelationshipType {
	    CONTAINS, EXPERIMENT
	}	
}