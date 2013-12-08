package by.bsuir.kslisenko.neo4j;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
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
import by.bsuir.kslisenko.util.handler.ConsoleReaderHandler;

/**
 * Imports clusters and points to neo4j
 * @author kslisenko
 */
public class Neo4jImporter {

//	private static final String SERVER = "http://54.204.44.21:7474/db/data";
	private static final String SERVER = "http://localhost:7474/db/data";
	private static final int BATCH_SIZE = 10000; // Objects imported per transaction
	private GraphDatabaseService graphDb;
	private String serverUri;
	private String pathToClusters;
	private String pathToDictionary;
	private String pathToPoints;
	
	private String[] dictionary;
	
	private Index<Node> clusterIndex;
	private Index<Node> pointsIndex;
	
	public Neo4jImporter(String serverUri, String pathToClusters, String pathToPoints, String pathToDictionary) {
		this.serverUri = serverUri;
		this.pathToClusters = pathToClusters;
		this.pathToDictionary = pathToDictionary;
		this.pathToPoints = pathToPoints;
	}
	
	public void doImport() throws IOException {
		graphDb = new RestGraphDatabase(serverUri);
		
		Configuration conf = new Configuration();
		dictionary = VectorHelper.loadTermDictionary(conf, pathToDictionary);
		
		// Import clusters
		addClusters(conf);
		
		// Import points
		addPoints(conf);
	}

	private void addPoints(Configuration conf) throws IOException {
		pointsIndex = graphDb.index().forNodes("points");
		
		// TODO use VectorHelper.vectorToSortedString(vector, dictionary)
		ReaderHandler<LongWritable, ClusteredDocument> handler = new ReaderHandler<LongWritable, ClusteredDocument>() {
			private int itemsInTransaction = 0;
			private Transaction currentTx;
			
			@Override
			public void before() throws IOException {
				currentTx = graphDb.beginTx();
			}

			@Override
			public void read(LongWritable key, ClusteredDocument value, PrintStream out) throws IOException {
				// Key = document ID
				out.println("Key: " + key);
				out.println("Cluster ID: " + value.getClusterId());
				out.println("Title: " + value.getDocumentTitle());
				out.println("Content: " + value.getDocumentContent());
				addPoint(key.toString(), value.getClusterId().toString(), value.getDocumentTitle().toString(), value.getDocumentContent().toString());
				
				itemsInTransaction++;
				if (itemsInTransaction >= BATCH_SIZE) {
					// Finish previous transaction and start new
					currentTx.success();
					currentTx.finish();
					currentTx = graphDb.beginTx();
				}				
			}

			@Override
			public void after() throws IOException {
				currentTx.success();
				currentTx.finish();				
			}
		};
		
		SequenceFileReaderUtil.readPartFilesInDir(pathToPoints, 10000000, conf, new ConsoleReaderHandler<LongWritable, ClusteredDocument>(handler));
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
		
		ReaderHandler<Text, Cluster> handler = new ReaderHandler<Text, Cluster>() {
			private int itemsInTransaction = 0;
			private Transaction currentTx;
			
			@Override
			public void before() throws IOException {
				currentTx = graphDb.beginTx();
			}

			@Override
			public void read(Text key, Cluster value, PrintStream out) throws IOException {
				out.println("Cluster id: " + value.getId());
				String name = getClusterName(value, dictionary);
				out.println(name);
				addCluster(value.getId() + "", name, value.getNumPoints(), rootNode);
				itemsInTransaction++;
				if (itemsInTransaction >= BATCH_SIZE) {
					// Finish previous transaction and start new
					currentTx.success();
					currentTx.finish();
					currentTx = graphDb.beginTx();
				}
			}

			@Override
			public void after() throws IOException {
				currentTx.success();
				currentTx.finish();
			}
		};
		SequenceFileReaderUtil.readPartFilesInDir(pathToClusters, 1000000, conf, new ConsoleReaderHandler<Text, Cluster>(handler));
	}
	
	private void addCluster(String id, String name, long numPoints, Node rootNode) {
		Node clusterNode = graphDb.createNode();
		clusterNode.setProperty("id", id);
		clusterNode.setProperty("name", name);
		clusterNode.setProperty("numPoints", numPoints);
		clusterIndex.add(clusterNode, "id", id);
		rootNode.createRelationshipTo(clusterNode, RelTypes.CONTAINS);
	}
	
	public static void main(String[] args) throws IOException {
		final String BASE = "../stackexchange-analyses-hadoop-mahout/target/stackoverflow-output-base/";
		Neo4jImporter importer = new Neo4jImporter(SERVER, 
				BASE + "kmeans/clusters-2-final",
				BASE + "clusteredPosts",
				BASE + "sparse/dictionary.file-*");
		importer.doImport();
	}
	
	private static class TermIndexWeight {
		private final int index;
		private final double weight;

		TermIndexWeight(int index, double weight) {
			this.index = index;
			this.weight = weight;
		}
	}
	
	protected String getClusterName(Cluster cluster, String[] dictionary) {
		List<TermIndexWeight> vectorTerms = new ArrayList<TermIndexWeight>();

		Iterator<Vector.Element> iter = cluster.getCenter().iterateNonZero();
		while (iter.hasNext()) {
			Vector.Element elt = iter.next();
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
	    CONTAINS
	}	
}