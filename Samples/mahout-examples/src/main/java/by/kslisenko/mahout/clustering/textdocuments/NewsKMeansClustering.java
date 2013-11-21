package by.kslisenko.mahout.clustering.textdocuments;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

public class NewsKMeansClustering {
	public static void main(String args[]) throws Exception {
		int minSupport = 2;
		int minDf = 5;
		int maxDFPercent = 95;
		
		// NGrams - are groups of words which have sense together (for example "Coca Cola" - word "Cola" would be always after word "Coca")
		// This number sets maximum words when searching this sentences
		int maxNGramSize = 2;
		int minLLRValue = 50;
		int reduceTasks = 1;
		int chunkSize = 200;
		int norm = 2;
		boolean sequentialAccessOutput = true;

		// Directory with text documents in mahout Sequence file format
		// It means that first we need to generate sequence file from input data
		String inputDir = "/home/cloudera/workspace/big-data-research/Samples/mahout-text-clustering/reuters-21578/reuters-seqfiles";
		String outputDir = "/home/cloudera/workspace/big-data-research/Samples/mahout-text-clustering/reuters-21578/out";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		HadoopUtil.delete(conf, new Path(outputDir));
		
		Path tokenizedPath = new Path(outputDir, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
		
		MyAnalyzer analyzer = new MyAnalyzer();
		// Converts a set of our input documents in the sequence file format
		// Convert the input documents into token array using the StringTuple The input documents has to be in the SequenceFile format
		// Retrieve words from input document, delete stop words?
		DocumentProcessor.tokenizeDocuments(new Path(inputDir), analyzer.getClass().asSubclass(Analyzer.class), tokenizedPath, conf);
		
		// This class converts a set of input documents in the sequence file format to vectors. 
		// The Sequence file input should have a Text key containing the unique document identifier and a StringTuple value containing the tokenized document. 
		// You may use DocumentProcessor to tokenize the document. This is a dictionary based Vectorizer.
		
		// Create Term Frequency (Tf) Vectors from the input set of documents in SequenceFile format. 
		// This tries to fix the maximum memory used by the feature chunk per node thereby splitting the process across multiple map/reduces.
		DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, new Path(outputDir), conf, minSupport, maxNGramSize,
				minLLRValue, 2, true, reduceTasks, chunkSize,
				sequentialAccessOutput, false);
		
		// This class converts a set of input vectors with term frequencies to TfIdf vectors. (Adds Inverse Document frequency feature)
		
		TFIDFConverter.processTfIdf(new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER), 
				new Path(outputDir), conf, chunkSize, minDf, maxDFPercent, norm, true,
				sequentialAccessOutput, false, reduceTasks);
		
		Path vectorsFolder = new Path(outputDir, "tfidf-vectors");
		Path canopyCentroids = new Path(outputDir, "canopy-centroids");
		Path clusterOutput = new Path(outputDir, "clusters");
		
		// Canopy clustering do not require initial number of clusters
		// Used to estimate number of clusters for K-Means clustering algorithm
		CanopyDriver.run(vectorsFolder, canopyCentroids, new EuclideanDistanceMeasure(), 250, 120, false, false);
		
		KMeansDriver.run(conf, vectorsFolder, new Path(canopyCentroids, "clusters-0"), 
				clusterOutput, new TanimotoDistanceMeasure(), 0.01, 20, true, false);
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(clusterOutput + "/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000"), conf);
		
		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		while (reader.next(key, value)) {
			System.out.println(key.toString() + " belongs to cluster " + value.toString());
		}
		
		reader.close();
	}
}