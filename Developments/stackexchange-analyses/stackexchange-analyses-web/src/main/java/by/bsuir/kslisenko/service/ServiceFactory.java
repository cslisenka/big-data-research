package by.bsuir.kslisenko.service;

/**
 * Simple IoC like container
 * @author cloudera
 */
public class ServiceFactory {
	
	private static final String NEO4J_URL = "http://localhost:7474/db/data";
//	private static final String NEO4J_URL = "http://50.16.193.54:7474/db/data";
	
	public static Neo4jDataService getNeo4j() {
		return new Neo4jDataService(NEO4J_URL);
	}
	
	public static ClusteredDocumentService getDocumentService() {
		return new ClusteredDocumentService(getNeo4j());
	}
	
	public static ClusterService getClusterService() {
		return new ClusterService(getNeo4j());
	}
}