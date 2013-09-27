package by.kslisenko.resourcestree.neo4j.model;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;

public class FileEntry {

	private String fullPath;
	private String name;
	
	public FileEntry(String fullPath) {
		this.fullPath = fullPath;
		this.name = new File(fullPath).getName();
	}
	
	public Node toNode(GraphDatabaseService graphDB, Index<Node> index) {
		Node node = graphDB.createNode();
		node.setProperty("fullpath", fullPath);
		node.setProperty("name", name);
		
		index.add(node, "fullpath", fullPath);
		return node;
	}
}