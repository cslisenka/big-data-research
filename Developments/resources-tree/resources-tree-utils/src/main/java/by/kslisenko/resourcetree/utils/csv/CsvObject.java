package by.kslisenko.resourcetree.utils.csv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvObject {

	private Map<String, String> attributes = new HashMap<String, String>();
	private List<String> attributeNames = new ArrayList<String>();

	public CsvObject(String... attributes) {
		attributeNames.addAll(Arrays.asList(attributes));
	}
	
	public void setAttribute(String name, String value) {
		if (!attributeNames.contains(name)) {
			throw new IllegalArgumentException("Object does not contain attribute with name " + name);
		}
		attributes.put(name, value);
	}

	public String getAttribute(String name) {
		return attributes.get(name);
	}
	
	public List<String> getAttributeNames() {
		return attributeNames;
	}
}