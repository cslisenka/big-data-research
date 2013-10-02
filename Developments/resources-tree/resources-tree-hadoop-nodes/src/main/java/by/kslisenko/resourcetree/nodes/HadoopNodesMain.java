package by.kslisenko.resourcetree.nodes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class HadoopNodesMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		// Create configuration
		Configuration conf = new Configuration(true);
		
		// Create job
		Job job = new Job(conf, "ResourceTreeNodes");
		job.setJarByClass(HadoopNodesMain.class);
		
		configureMapReduceJob(job);
		
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}
	
	public static void configureMapReduceJob(Job job) {
		// Setup map reduce
		job.setMapperClass(ResourceTreeNodesMapper.class);
		job.setReducerClass(ResourceTreeNodesReducer.class);
		job.setNumReduceTasks(1);
		
		// Specify key/value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Input
		job.setInputFormatClass(TextInputFormat.class);
		
		// Output
		job.setOutputFormatClass(TextOutputFormat.class);
	}
}