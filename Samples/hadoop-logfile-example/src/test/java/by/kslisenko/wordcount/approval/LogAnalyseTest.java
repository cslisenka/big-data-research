package by.kslisenko.wordcount.approval;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.approvaltests.hadoop.version1.HadoopApprovals;
import org.approvaltests.hadoop.version1.MapperWrapper;
import org.approvaltests.hadoop.version1.ReducerWrapper;
import org.approvaltests.hadoop.version1.SmartMapper;
import org.approvaltests.hadoop.version1.SmartReducer;
import org.junit.Before;
import org.junit.Test;

import by.kslisenko.wordcount.LogAnalyseMapper;
import by.kslisenko.wordcount.LogAnalyseReducer;

/**
 * Approval tests visualize map-reduce task results in text-files. It's good to understand what's happens.
 * 
 * @author kslisenko
 */
public class LogAnalyseTest {

	private String testLog;
	
	@Before
	public void setUp() throws IOException {
		testLog = FileUtils.readFileToString(new File("src/main/resources/tcpdump.list"));
	}
	
	@Test
	public void testMapReduce() throws Exception {
		
		SmartMapper<Object, Text, Text, Text> smartMapper = 
				new MapperWrapper<Object, Text, Text, Text>(new LogAnalyseMapper(), Object.class, Text.class, Text.class, Text.class);
		
		SmartReducer<Text, Text, Text, IntWritable> smartReducer = 
				new ReducerWrapper<Text, Text, Text, IntWritable>(new LogAnalyseReducer(), Text.class, Text.class, Text.class, IntWritable.class);
		
		HadoopApprovals.verifyMapReduce(smartMapper, smartReducer, 1, testLog);
	}
}