package by.kslisenko.resourcetree.utils.model.graph;

import by.kslisenko.resourcetree.utils.csv.CsvObject;

public class FileGraphNode extends CsvObject {
	
	public FileGraphNode(String fullpath) {
		super("fullpath:string:files", "shortname");
		setAttribute("fullpath:string:files", fullpath);
	}
}
