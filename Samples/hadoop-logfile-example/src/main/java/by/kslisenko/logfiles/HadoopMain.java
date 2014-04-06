package by.kslisenko.logfiles;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

public class HadoopMain {
	public static void main(String[] args) throws Exception {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		// Create configuration
		Configuration conf = new Configuration(true);
		// Create job
		
		// Testing filesystem
		FileSystem fs = FileSystem.get(in.toUri(), conf);
		conf.writeXml(System.out);
		System.out.println("in.toUri()=" + in.toUri());
		System.out.println("fs.getHomeDirectory()=" + fs.getHomeDirectory());
		System.out.println("fs.getUri()=" + fs.getUri());
		System.out.println("fs.getWorkingDirectory()=" + fs.getWorkingDirectory());
		System.out.println("FileSystem.getDefaultUri(conf)=" + FileSystem.getDefaultUri(conf));
		System.out.println("fs.getClass().getName()=" + fs.getClass().getName());
		System.out.println("fs instanceof S3FileSystem" + (fs instanceof S3FileSystem));
		System.out.println("fs instanceof NativeS3FileSystem" + (fs instanceof NativeS3FileSystem));
		System.out.println("fs instanceof DistributedFileSystem" + (fs instanceof DistributedFileSystem));
		System.out.println("fs.getFileStatus(in).isDir()=" + fs.getFileStatus(in).isDir());
		
		RandomSeedGenerator.buildRandom(conf, new Path(args[0]), new Path(args[1]), 10, new EuclideanDistanceMeasure());
		
//		Job job = new Job(conf, "LogAnalyses");
//		job.setJarByClass(HadoopMain.class);
//		configureHostProtocolCountMapReduceJob(job);
//		FileInputFormat.addInputPath(job, in);
//		FileOutputFormat.setOutputPath(job, out);
//		// Execute job
//		int code = job.waitForCompletion(true) ? 0 : 1;
//		System.exit(code);
	}
	
	public static void configureHostProtocolCountMapReduceJob(Job job) {
		// Setup map reduce
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(LogReducer.class);
		job.setNumReduceTasks(1);
		// Specify key/value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// Input
		job.setInputFormatClass(TextInputFormat.class);
		// Output
		job.setOutputFormatClass(TextOutputFormat.class);
	}
}