package by.kslisenko.resourcetree.utils.model.graph;

import by.kslisenko.resourcetree.utils.csv.CsvObject;

public class HostGraphNode extends CsvObject {

	public HostGraphNode(String hostname) {
		super("fullpath:string:files", "shortname");
		setAttribute("shortname", hostname);
	}
}
