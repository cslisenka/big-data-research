package by.kslisenko.resourcetree.utils.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvOutputCombiner {
	
	public static final String SEPARATOR = "	";

	private List<String> attributes = new ArrayList<String>();
	private Map<String, String> renames = new HashMap<String, String>();
	
	public void addTemplateObject(CsvObject object) {
		attributes.addAll(object.getAttributeNames());
	}
	
	public String generateRow(CsvObject object) {
		List<String> resultAttributes = new ArrayList<String>();
		for (String attribute : attributes) {
			resultAttributes.add(emptyIfNull(object.getAttribute(attribute)));
		}
		
		return printListWithSeparator(resultAttributes, SEPARATOR);
	}
	
	public String generateHeader() {
		return printListWithSeparator(rename(attributes, renames), SEPARATOR);
	}
	
	public void renameHeader(String attrName, String headerName) {
		renames.put(attrName, headerName);
	}
	
	protected String emptyIfNull(String value) {
		return value != null ? value : "";
	}
	
	protected static String printListWithSeparator(List<String> list, String separator) {
		StringBuilder result = new StringBuilder();
		
		for (String value : list) {
			result.append(value)
				.append(separator);
		}
		
		return result.toString().trim();		
	}
	
	protected static List<String> rename(List<String> listToRename, Map<String, String> renames) {
		List<String> results = new ArrayList<String>();
		for (String value : listToRename) {
			results.add(renames.get(value) == null ? value : renames.get(value));
		}
		
		return results;
	}
}