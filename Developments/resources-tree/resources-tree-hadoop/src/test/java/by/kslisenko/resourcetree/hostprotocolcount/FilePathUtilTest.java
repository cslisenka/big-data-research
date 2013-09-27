package by.kslisenko.resourcetree.hostprotocolcount;

import java.util.Map;

import org.junit.Test;

import junit.framework.Assert;

public class FilePathUtilTest {

	@Test
	public void testParentChildPairs() {
		Map<String, String> results = FilePathUtil.getParentChildPairs("/dir1/dir2/dir3/file1.ext");
		Assert.assertEquals(5, results.size());
	}
}