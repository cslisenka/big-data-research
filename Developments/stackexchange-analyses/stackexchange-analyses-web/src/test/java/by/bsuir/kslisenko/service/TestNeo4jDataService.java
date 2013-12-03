package by.bsuir.kslisenko.service;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.neo4j.graphdb.Node;

import by.bsuir.kslisenko.model.ClusteredDocument;

public class TestNeo4jDataService {

	@Test
	public void testGetClusters() {
		Neo4jDataService service = new Neo4jDataService("http://localhost:7474/db/data");
		List<Node> nodes = service.getNodes("clusters");
		Assert.assertTrue(nodes.size() > 0);
		for (Node node : nodes) {
			System.out.println(node.getProperty("id"));
		}
	}
	
	@Test
	public void testClusteredDocuments() {
		Neo4jDataService service = new Neo4jDataService("http://localhost:7474/db/data");
		ClusteredDocumentService docService = new ClusteredDocumentService(service);
		List<ClusteredDocument> docs = docService.getClusteredDocuments("8035");
		Assert.assertTrue(docs.size() > 0);
		for (ClusteredDocument node : docs) {
			System.out.println(node.getTitle());
		}		
	}
}