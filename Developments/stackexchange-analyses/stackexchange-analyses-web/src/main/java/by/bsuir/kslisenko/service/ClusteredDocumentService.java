package by.bsuir.kslisenko.service;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import by.bsuir.kslisenko.model.ClusteredDocument;

public class ClusteredDocumentService extends Neo4jUsingService {

	public ClusteredDocumentService(Neo4jDataService graph) {
		super(graph);
	}
	
	public List<ClusteredDocument> getClusteredDocuments(String clusterId) {
		List<ClusteredDocument> result = new ArrayList<ClusteredDocument>();
		Node cluster = neo4j.getNode("clusters", "id", clusterId);
		if (cluster != null) {
			for (Relationship rel : cluster.getRelationships()) {
				result.add(fromNode(rel.getEndNode()));
			}
		}
		return result;
	}
	
	public ClusteredDocument fromNode(Node node) {
		ClusteredDocument result = new ClusteredDocument();
		result.setId(saveGetProperty(node, "id"));
		result.setTitle(saveGetProperty(node, "title"));
		result.setContent(saveGetProperty(node, "content"));
		return result;
	}
}