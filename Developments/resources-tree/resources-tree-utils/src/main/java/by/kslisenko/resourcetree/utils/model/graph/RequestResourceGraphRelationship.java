package by.kslisenko.resourcetree.utils.model.graph;

import by.kslisenko.resourcetree.utils.csv.CsvObject;

public class RequestResourceGraphRelationship extends CsvObject {

	public RequestResourceGraphRelationship() {
		super("parent", "child", "type");
	}
}