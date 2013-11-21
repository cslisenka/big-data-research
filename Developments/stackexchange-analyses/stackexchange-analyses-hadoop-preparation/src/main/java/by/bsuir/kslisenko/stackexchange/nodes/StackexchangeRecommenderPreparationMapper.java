
package by.bsuir.kslisenko.stackexchange.nodes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StackexchangeRecommenderPreparationMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (value.toString().trim().startsWith("<row")) {
			Map<String, String> keyValuePairs = xmlRowToKeyValuePairs(value.toString());
			String postType = keyValuePairs.get("PostTypeId");
			if (postType != null && postType.equals("2")) {
				String owner = keyValuePairs.get("OwnerUserId");
                String parent = keyValuePairs.get("ParentId");
                if (owner != null && parent != null) {
                	context.write(new Text(owner), new Text(parent));
                }
			}
			
//			NodeList l = doc.getElementsByTagName("row");
//			Element row = (Element) l.item(0);
//			// Take only answers
//			if ("2".equals(row.getAttribute("PostTypeId"))) {
//				String owner = row.getAttribute("OwnerUserId");
//                String parent = row.getAttribute("ParentId");
//                if (owner != null && parent != null) {
//                	context.write(new Text(owner), new Text(parent));
//                }
//			}
		}
	}
	
	public Map<String, String> xmlRowToKeyValuePairs(String row) {
		Map<String, String> keyValuePairs = new HashMap<String, String>();
		Pattern p = Pattern.compile("([^\\s=]*)=\"([^\\s=]*)\"");
		Matcher m = p.matcher(row);
		while (m.find()) {
			keyValuePairs.put(m.group(1), m.group(2));
		}
		return keyValuePairs;
	}
}