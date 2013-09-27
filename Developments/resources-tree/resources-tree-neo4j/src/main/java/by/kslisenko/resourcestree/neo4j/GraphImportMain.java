package by.kslisenko.resourcestree.neo4j;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rest.graphdb.RestGraphDatabase;

public class GraphImportMain {

	public static void main(String[] args) throws IOException {
		if (args.length < 2 || args[0].isEmpty() || args[1].isEmpty()) {
			throw new IllegalArgumentException("Path to import file and neo4j server uri are not specified in command line arguments. " +
					"Example: <executable> filetoimport.txt http://localhost:7474/db/data");
		}
		
		GraphDatabaseService graphDb = new RestGraphDatabase(args[1]);
		GraphImporter importer = new GraphImporter(graphDb);
		importer.importFromFile(new File(args[0]));
	}
}