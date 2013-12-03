package by.bsuir.kslisenko.service;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.rest.graphdb.RestGraphDatabase;

public class Neo4jDataService {

	private GraphDatabaseService graphDb;
	
	public Neo4jDataService(String serverUri) {
		graphDb = new RestGraphDatabase(serverUri);
	}

	public Node getNode(String indexName, String key, String value) {
		return graphDb.index().forNodes(indexName).get(key, value).getSingle();
	}	
	
	public List<Node> getNodes(String indexName, String key, String value) {
		List<Node> result = new ArrayList<Node>();
		IndexHits<Node> hits = graphDb.index().forNodes(indexName).get(key, value);
		for (Node node : hits) {
			result.add(node);
		}
		return result;
	}
	
	public List<Node> getNodes(String indexName) {
		List<Node> result = new ArrayList<Node>();
		IndexHits<Node> hits = graphDb.index().forNodes(indexName).query("*:*");
		for (Node node : hits) {
			result.add(node);
		}
		return result;		
	}
}