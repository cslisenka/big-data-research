package by.kslisenko.resourcetree.utils.model.graph;

import by.kslisenko.resourcetree.utils.csv.CsvOutputCombiner;

public class RelationshipCombiner extends CsvOutputCombiner {

	public RelationshipCombiner() {
		renameHeader("parent", "name:string:files");
		renameHeader("child", "name:string:files");
	}
}
