package by.kslisenko.resourcetree.utils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class FilePathUtil {

	public static Map<String, String> getParentChildPairs(String filePath) {
		Map<String, String> result = new HashMap<String, String>();
		File file = new File(filePath);
		while (file.getParentFile() != null) {
			result.put(file.getParent(), file.getPath());
			file = file.getParentFile();
		}
		
		result.put("", "/");
		
		return result;
	}
}