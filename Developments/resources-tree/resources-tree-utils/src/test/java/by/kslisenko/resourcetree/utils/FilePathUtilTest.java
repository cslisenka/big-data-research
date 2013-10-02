package by.kslisenko.resourcetree.utils;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class FilePathUtilTest {

	@Test
	public void testParentChildPairs() {
		Map<String, String> results = FilePathUtil.getParentChildPairs("/dir1/dir2/dir3/file1.ext");
		Assert.assertEquals(5, results.size());
	}
}