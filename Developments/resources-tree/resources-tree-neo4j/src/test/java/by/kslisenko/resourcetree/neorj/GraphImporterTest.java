package by.kslisenko.resourcetree.neorj;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rest.graphdb.RestGraphDatabase;

import by.kslisenko.resourcestree.neo4j.GraphImporter;

public class GraphImporterTest {

	private GraphDatabaseService graphDb;
	private GraphImporter importer;
	
	@Before
	public void setUp() {
		graphDb = new RestGraphDatabase("http://localhost:7474/db/data");
		importer = new GraphImporter(graphDb);
	}
	
	@Test
	public void testImportSimpleGraph() throws IOException {
		importer.importFromFile(new File("src/test/resources/part-r-00000"));
	}
}