package by.bsuir.kslisenko.service;

import org.neo4j.graphdb.Node;

public class Neo4jUsingService {

	protected Neo4jDataService neo4j;
	
	public Neo4jUsingService(Neo4jDataService neo4j) {
		this.neo4j = neo4j;
	}

	public Neo4jDataService getNeo4j() {
		return neo4j;
	}

	public void setNeo4j(Neo4jDataService neo4j) {
		this.neo4j = neo4j;
	}
	
	protected String saveGetProperty(Node node, String name) {
		if (node.hasProperty(name)) {
			return node.getProperty(name).toString();
		}
		
		return "";
	}
}