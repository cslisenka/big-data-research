package by.kslisenko.resourcestree.neo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

import by.kslisenko.resourcestree.neo4j.model.DirTreeRelType;
import by.kslisenko.resourcestree.neo4j.model.FileEntry;

public class GraphImporter {
	
	private GraphDatabaseService graphDb;
	private Index<Node> resourceIndex;
	
	public GraphImporter(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
	}

	public void importFromFile(File file) throws IOException {
		// Open file to imput
		BufferedReader reader = new BufferedReader(new FileReader(file));
		try {
			String line = null;
			
			// Create index
			resourceIndex = graphDb.index().forNodes("resourceIndex");
			
			while ((line = reader.readLine()) != null) {
				importLine(line);
			}
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}
	}
	
	protected void importLine(String line) {
		// 1. Parse line
		Pattern p = Pattern.compile("([\\S]*)\\s*([\\S]*)");
		Matcher m = p.matcher(line);
		if (m.find()) {
			String parentNode = m.group(1);
			String childNode = m.group(2);
			createNodeAndRelate(parentNode, childNode);
		}
	}
		
	/**
	 * Creates graph node for new entity and relates it with parent.
	 * Imports each node-relationship in a single transaction.
	 * 
	 * @param parentId
	 * @param nodeId
	 */
	protected void createNodeAndRelate(String parentId, String nodeId) {
		Node parentNode = resourceIndex.get("fullpath", parentId).getSingle();
		
		Transaction tx = graphDb.beginTx();
		try {
			Node fileNode = new FileEntry(nodeId).toNode(graphDb, resourceIndex);
			System.out.println("Create node " + nodeId);
			if (parentNode != null) {
				fileNode.createRelationshipTo(parentNode, DirTreeRelType.BELONGS);
				System.out.println("create relationship from " + nodeId + "to " + parentId + " (" + parentNode.getId() + ")");
			}
			
			// Add index
		    tx.success();
		} finally {
		    tx.finish();
		}
	}
}