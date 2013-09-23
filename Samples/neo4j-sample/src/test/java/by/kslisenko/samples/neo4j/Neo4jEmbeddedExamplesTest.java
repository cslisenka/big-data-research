package by.kslisenko.samples.neo4j;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;

public class Neo4jEmbeddedExamplesTest {

	private GraphDatabaseService graphDb;

	private static enum RelTypes implements RelationshipType
	{
	    LEARNS
	}
	
	@Before
	public void setUp() {
		// Create embedded database server (good to be used in unit-tests)
		Map<String, String> config = new HashMap<String, String>();
		config.put("neostore.nodestore.db.mapped_memory", "10M");
		config.put("string_block_size", "60");
		config.put("array_block_size", "300");
		graphDb = new GraphDatabaseFactory()
		    .newEmbeddedDatabaseBuilder("target/database")
		    .setConfig(config)
		    .newGraphDatabase();
	}

	@Test
	public void testAddDataFindByLegacyIndex() {
		// Create index with name nodesIndex
		Index<Node> nodeIndex = graphDb.index().forNodes("nodesIndex");
		
		Transaction tx = graphDb.beginTx();
		try {
			Node kostyaNode = graphDb.createNode();
			kostyaNode.setProperty("name", "Kostya");
			kostyaNode.setProperty("type", "developer");
			
			Node neo4jNode = graphDb.createNode();
			neo4jNode.setProperty("name", "Neo4j");
			neo4jNode.setProperty("type", "database");
			
			Relationship relationship = kostyaNode.createRelationshipTo(neo4jNode, RelTypes.LEARNS);
			relationship.setProperty("way", "creating samples");
			
			// Associate key=>value pair with kostya node
			// TODO why there is a key=>value, but not a simple key?
			nodeIndex.add(kostyaNode, "key", "value");
			
		    tx.success();
		} finally {
		    tx.finish();
		}
		
		// Read data
		Node searchedNode = nodeIndex.get("key", "value").getSingle();
		Assert.assertEquals("Kostya", searchedNode.getProperty("name"));
		Assert.assertEquals("developer", searchedNode.getProperty("type"));
	}

	@After
	public void tearDown() {
		graphDb.shutdown();
	}
}