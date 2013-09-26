package by.kslisenko.wordcount.neo4j;

/**
 * Created with IntelliJ IDEA.
 * User: fbmnds
 * Date: 26.09.13
 * Time: 11:09
 * To change this template use File | Settings | File Templates.
 */


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.rest.graphdb.RestGraphDatabase;

/**
 * This example demonstrates using neo4j-rest-graphdb library for interacting with neo4j external server.
 * Neo4j-rest-graphdb library wraps java API calls into REST API calls.
 *
 * It's better to use this way than execute REST calls manually with Jersey:
 * 1. This java API is better understandable than REST API;
 * 2. Same code could be used both for unit-testing (with embedded server) and for production (with external server);
 *
 * @author kslisenko
 */
public class Neo4jConnector {

    private static final String SERVER_URI = "http://localhost:7474/db/data";
    private GraphDatabaseService graphDb;

    private static enum RelTypes implements RelationshipType {
        LEARNS
    }

    @Before
    public void setUp() {
        graphDb = new RestGraphDatabase(SERVER_URI);
    }

    @Test
    public void testCRUD() {
        // Create index with name nodesIndex
        Index<Node> nodeIndex = graphDb.index().forNodes("nodesIndex");

        Transaction tx = graphDb.beginTx();
        try {
            Node kostyaNode = graphDb.createNode();
            kostyaNode.setProperty("name", "Kostya");
            kostyaNode.setProperty("type", "developer");
            kostyaNode.setProperty("age", "23");

            System.out.println("Kostya node id = " + kostyaNode.getId());

            Node neo4jNode = graphDb.createNode();
            neo4jNode.setProperty("name", "Neo4j");
            neo4jNode.setProperty("type", "database");
            neo4jNode.setProperty("server_type", "external");

            Relationship relationship = kostyaNode.createRelationshipTo(neo4jNode, RelTypes.LEARNS);
            relationship.setProperty("way", "creating samples");
            relationship.setProperty("tools_to_access", "rest api");

            // Associate key=>value pair with kostya and neo4j nodes
            // After accociation we can search this nodes by index
            // TODO why there is a key=>value, but not a simple key?
            nodeIndex.add(kostyaNode, "key", "value");
            nodeIndex.add(neo4jNode, "key", "value2");

            tx.success();
        } finally {
            tx.finish();
        }

        // Find kostya node by full key and value pair index
        Node searchedNode = nodeIndex.get("key", "value").getSingle();

        Assert.assertEquals("Kostya", searchedNode.getProperty("name"));
        Assert.assertEquals("developer", searchedNode.getProperty("type"));

        // Querying the data, find all nodes with "key" index key
        Assert.assertTrue(nodeIndex.query("key", "*").size() > 0);

        // Delete node
        Transaction tx2 = graphDb.beginTx();
        try {
            // First we need to delete all relationships of node, otherwise
            // deletion throws RuntimeException
            searchedNode.getSingleRelationship(RelTypes.LEARNS, Direction.BOTH).delete();
            searchedNode.delete();
            tx2.success();
        } finally {
            tx2.finish();
        }

        // Afrer deleting node we can not find it by index
        Assert.assertNull(nodeIndex.get("key", "value").getSingle());
    }
}