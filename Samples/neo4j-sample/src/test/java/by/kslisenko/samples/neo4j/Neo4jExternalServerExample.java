package by.kslisenko.samples.neo4j;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Example shows process of interaction with remote neo4j server via REST API 
 * manually using Jersey REST services wrapper.
 * This way is not very good because we need to build json manually.
 * It's better to use Neo4jExternalServerRestBindingExample way.
 * 
 * @author kslisenko
 */
public class Neo4jExternalServerExample {

	public static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
	public static final String NODE_ENTRY_POINT_URI = SERVER_ROOT_URI + "node";

	@Before
	public void setUp() {
		checkNeo4jServerRun();
	}

	@Test
	public void testCRUD() throws URISyntaxException {
		URI kostyaNode = createNode();
		addProperty(kostyaNode, "name", "Kostya");
		addProperty(kostyaNode, "type", "user");
		
		URI neo4jNode = createNode();
		addProperty(neo4jNode, "name", "Neo4j");
		addProperty(neo4jNode, "type", "graph database");
		
		addRelationship(kostyaNode, neo4jNode, "learns", "{ \"activities\" : \"create samples\" }" );
	}
	
	protected URI addRelationship(URI startNode, URI endNode, String type, String jsonAttributes) throws URISyntaxException {
		URI fromUri = new URI(startNode.toString() + "/relationships");
		String relationshipJson = generateJsonRelationship(endNode, type, jsonAttributes);

		WebResource resource = Client.create().resource(fromUri);
		
		// POST JSON to the relationships URI
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity(relationshipJson)
				.post(ClientResponse.class);

		final URI location = response.getLocation();
		System.out.println(String.format(
				"POST to [%s], status code [%d], location header [%s]",
				fromUri, response.getStatus(), location.toString()));

		response.close();
		return location;
	}
	
    private static String generateJsonRelationship( URI endNode,
            String relationshipType, String... jsonAttributes )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "{ \"to\" : \"" );
        sb.append( endNode.toString() );
        sb.append( "\", " );

        sb.append( "\"type\" : \"" );
        sb.append( relationshipType );
        if ( jsonAttributes == null || jsonAttributes.length < 1 ) {
            sb.append( "\"" );
        }
        else {
            sb.append( "\", \"data\" : " );
            for ( int i = 0; i < jsonAttributes.length; i++ ) {
                sb.append( jsonAttributes[i] );
                if ( i < jsonAttributes.length - 1 ) { // Miss off the final comma
                    sb.append( ", " );
                }
            }
        }

        sb.append( " }" );
        System.out.println(sb.toString());
        return sb.toString();
    }	

	protected void addProperty(URI nodeUri, String key, String value) {
		String propertyUri = nodeUri.toString() + "/properties/" + key;
		// http://localhost:7474/db/data/node/{node_id}/properties/{property_name}

		WebResource resource = Client.create().resource(propertyUri);
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity("\"" + value + "\"")
				.put(ClientResponse.class);

		System.out.println(String.format("PUT to [%s], status code [%d]",
				propertyUri, response.getStatus()));
		response.close();
	}

	protected URI createNode() {
		WebResource resource = Client.create().resource(NODE_ENTRY_POINT_URI);
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity("{}")
				.post(ClientResponse.class);

		final URI location = response.getLocation();
		System.out.println(String.format(
						"POST to [%s], status code [%d], location header [%s]",
						NODE_ENTRY_POINT_URI, response.getStatus(),
						location.toString()));
		response.close();
		return location;
	}

	/**
	 * Checks Neo4j server availability. Fails unit-test available. 
	 * Sends HTTP request and verifies status.
	 */
	protected void checkNeo4jServerRun() {
		WebResource resource = Client.create().resource(SERVER_ROOT_URI);
		ClientResponse response = resource.get(ClientResponse.class);
		int status = response.getStatus();
		response.close();
		if (status != 200) {
			Assert.fail("Neo4j server at " + SERVER_ROOT_URI + " is not available");
		}
	}
}