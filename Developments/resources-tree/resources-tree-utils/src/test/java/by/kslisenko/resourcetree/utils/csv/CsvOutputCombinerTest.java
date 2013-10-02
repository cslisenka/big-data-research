package by.kslisenko.resourcetree.utils.csv;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class CsvOutputCombinerTest {

	protected CsvObject object1;
	protected CsvObject object2;
	protected CsvOutputCombiner combiner;
	
	@Before
	public void setUp() {
		object1 = new CsvObject("a", "b", "c");
		object1.setAttribute("a", "aa");
		object1.setAttribute("b", "bb");
		
		object2 = new CsvObject("d", "e", "f");
		object2.setAttribute("d", "dd");
		object2.setAttribute("f", "ff");
		
		combiner = new CsvOutputCombiner();
	}
	
	@Test
	public void testGenerateHeader() {
		combiner.addTemplateObject(object1);
		
		Assert.assertEquals("a	b	c", combiner.generateHeader());
		
		combiner.addTemplateObject(object2);
		
		Assert.assertEquals("a	b	c	d	e	f", combiner.generateHeader());
	}
	
	@Test
	public void testGenerateRow() {
		combiner.addTemplateObject(object1);
		Assert.assertEquals("aa	bb", combiner.generateRow(object1));
		
		combiner.addTemplateObject(object2);
		Assert.assertEquals("dd		ff", combiner.generateRow(object2));
	}
	
	@Test
	public void testRenames() {
		combiner.addTemplateObject(object1);
		combiner.renameHeader("a", "newa");
		Assert.assertEquals("newa	b	c", combiner.generateHeader());
	}
}