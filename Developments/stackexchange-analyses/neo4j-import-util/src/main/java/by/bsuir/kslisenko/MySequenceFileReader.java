package by.bsuir.kslisenko;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.vectors.VectorHelper;
import org.apache.mahout.vectorizer.collocations.llr.Gram;

import bbuzz2011.stackoverflow.join.ClusteredDocument;
import bbuzz2011.stackoverflow.preprocess.xml.PostWritable;
import by.bsuir.kslisenko.util.ReaderHandler;
import by.bsuir.kslisenko.util.SequenceFileReaderUtil;
import by.bsuir.kslisenko.util.handler.ConsoleReaderHandler;
import by.bsuir.kslisenko.util.handler.SimpleConsoleReaderHandler;
import by.bsuir.kslisenko.util.handler.TextFileOutputReaderHandler;

/**
 * Preparing and clustering data process produces several binary sequence files.
 * This utility reads and output this files data to console for better understanding what's going on.
 * 
 * @author kslisenko
 */
public class MySequenceFileReader {

	// Max records for output to textfile
	private static final int TEXT_FILE_MAX_ROWS = 1000000; //0
	// Number of documents to show at console
	private static final int DOCUMENTS_COUNT = 10;	

	private static final String BASE = "../stackexchange-analyses-hadoop-mahout/target/stackoverflow-output-base/";

	/**
	 * Dictionary in memory for better vectors visualization.
	 */
	public static String[] dictionary;
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		
		System.setOut(new PrintStream(new FileOutputStream(new File("target/out.txt"))));
		
		// 1. Preprocess data
		
		// 1.1 StackOverflowPostXMLParserJob
		// posts.xml -> sequence file [PostId] [PostWritable(Title Text)]
		// This data needed after clustering job finish for indexing. We will join posts with cluster IDs.
		readProcessedPostsToPostWritable(conf, BASE + "posts");
		
		// 1.2 StackOverflowPostTextExtracterJob 
		// sequence file [PostId] [PostWritable(Title Text)] -> sequence file [PostId] [Title+Text]
		// Source data for vectorizing and clustering
		readProcessedPosts(conf, BASE + "posts-text");
		
		// 2. Vectorize data

		// 2.1 Read tokenized documents
		// For document 
		// document ["What is the best way to micro-adjust a lens? I have a Canon 7D with a 50mm f/1.4 lens, and I think the auto-focus of the lens is off. How can I test and adjust this reliably?"]
		// Result would be [micro, adjust, lens, canon, lens, think, auto, focus, lens, test, adjust, reliably, will, approach, work, lenses, different, camera, body, other, different, options]
		// using org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles
		readTokenizedDocuments(conf, BASE + "sparse/tokenized-documents");
		
		// 2.2 Output generated dictionary from tokenized documents
		// Dictionary contains all words in all documents and their IDs
		// abandoned	0
		// abberation	1
		// abbreviations	2
		// aberration	3
		// aberrations	4
		dictionary = readDictionary(conf, BASE + "sparse/dictionary.file-0");
		

		// 2.3 Output frequency file
		// TODO what does this files represent?
		// TODO at which stage does this file created?
		readFrequencyFile(conf, BASE + "sparse/frequency.file-0");
		readDfCount(conf, BASE + "sparse/df-count");		
		
		// 2.4 Output generated vectors
		// Each vector represent document
		// TF vector [action:1.0,advantages:2.0,after:1.0,among:1.0...]
		// IDF vector [emphasis:0.17316427777013857,fatigue:0.2023817443352598,greg:0.19089462250730813...]
		readTfVectors(conf, BASE + "sparse/tf-vectors");
		readTfIdfVectors(conf, BASE + "sparse/tfidf-vectors");
		
		// 2.5 Read ngrams - words or couples of words with their frequency
		// abandoned	2.0
		// abberation	2.0
		// abbreviations	2.0
		// aberration	53.0
		// aberrations	11.0
		readNGrams(conf, BASE + "sparse/wordcount/ngrams");
		
		// 2.6 Read subgrams - relations between ngrams
		// gram: aberration correction [frq=3, type=n]	 has gram: aberration [frq=53, type=h]
		readSubgrams(conf, BASE + "sparse/wordcount/subgrams");
		
		// 3. Cluster data		
		// using org.apache.mahout.clustering.kmeans.KMeansDriver
		
		// Clustering
		// 3.1 Read initial clusters
		// Randomly selected initial clusters. Each cluster has a centroid with common terms.
		// [customised:0.2927406505146588,fields:0.23116300838484566,opensource:0.27911637996861227...]
		readInitialClusters(conf, "target/stackoverflow-kmeans-initial-clusters");
		
		// 3.2 Read clustered points
		// Points represent text documents
		readClusteredPoints(conf, BASE + "kmeans/clusteredPoints");
		
		// 3.3 Final clusters
		// Same output structure after initial clusters
		readFinalClusters(conf, BASE + "kmeans/clusters-2-final");
		
		// 3.4 Read canopy clusters
		readCanopyClusters(BASE + "canopy/clusters-0-final", conf);

		// 3.5 Read fuzzy k-means clistering
		readClusters(BASE + "fuzzy-kmeans/clusters-1-final", conf);
		readClusters(BASE + "kmeans/clusters-1-final", conf);
		
		// 4. Post process data
		// 4.1 Read points to clusters mapping (information taken from clusters and points files)
		// PointToClusterMappingJob
		readPointsToClustersMapping(conf, BASE + "pointsToClusters_kmeans");
		readPointsToClustersMapping(conf, BASE + "pointsToClusters_fuzzy-kmeans");
		
		// 4.2 Read clustered posts file
		// ClusterJoinerJob
		readClusteredPosts(conf, BASE + "clusteredPosts_kmeans");
		readClusteredPosts(conf, BASE + "clusteredPosts_fuzzy-kmeans");
		
		// Read inter-cluster distance
		
//		conf.set(RepresentativePointsDriver.DISTANCE_MEASURE_KEY, CosineDistanceMeasure.class.getName());
//		ClusterEvaluator evaluator = new ClusterEvaluator(conf, new Path(BASE + "kmeans/clusters-1-final"));
//		
//		Map<Integer, Vector> dist = evaluator.interClusterDistances();
//		for (Integer key : dist.keySet()) {
//			System.out.println(key + ":" + dist.get(key));
//		}
		
//		ClusterDumper clusterDumper = new ClusterDumper(new Path(output, "clusters-*-final"), new Path(output,
//                "clusteredPoints"));
		
		// Evaluate distances between clusters
		countClusterDistances(conf, BASE + "kmeans/clusters-1-final");
	}

	private static void countClusterDistances(Configuration conf, String path) throws IOException {
		final List<Cluster> clusters = new ArrayList<Cluster>();
		
		ReaderHandler<IntWritable, ClusterWritable> handler = new ReaderHandler<IntWritable, ClusterWritable>() {
			@Override
			public void before() throws IOException {}

			@Override
			public void read(IntWritable key, ClusterWritable value, PrintStream out) throws IOException {
				clusters.add(value.getValue());
			}

			@Override
			public void after() throws IOException {}
		};
		SequenceFileReaderUtil.readPartFilesInDir(path, TEXT_FILE_MAX_ROWS, conf, handler);
		
		
		for (Cluster c : clusters) {
			for (Cluster c2 : clusters) {
				double distance = VectorDistanceUtil.getCosineDistance(c.getCenter(), c2.getCenter());
				System.out.println("Cluster " + c.getId() + " <-> " + c2.getId() + " = " + distance);
			}
		}
		System.out.println("");
	}

	private static void readClusteredPosts(Configuration conf, String path) throws IOException {
		// TODO use VectorHelper.vectorToSortedString(vector, dictionary)
		ReaderHandler<LongWritable, ClusteredDocument> handler = new ReaderHandler<LongWritable, ClusteredDocument>() {
			@Override
			public void before() throws IOException {
			}

			@Override
			public void read(LongWritable key, ClusteredDocument value, PrintStream out) throws IOException {
				// Key = document ID
				out.println("Key: " + key);
				out.println("Cluster ID: " + value.getClusterId());
				out.println("Title: " + value.getDocumentTitle());
				out.println("Content: " + value.getDocumentContent());
				out.println();
			}

			@Override
			public void after() throws IOException {
			}
		};
		
		SequenceFileReaderUtil.readPartFilesInDir(path, DOCUMENTS_COUNT, conf, new ConsoleReaderHandler<LongWritable, ClusteredDocument>(handler));
		
		SequenceFileReaderUtil.readPartFilesInDir(path, TEXT_FILE_MAX_ROWS, conf, new TextFileOutputReaderHandler<LongWritable, ClusteredDocument>(path + "/clusteredDocuments.txt", handler));
	}

	private static void readPointsToClustersMapping(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFilesInDirToConsole(path, 20, conf);
		
		ReaderHandler<LongWritable, IntWritable> handler = new ReaderHandler<LongWritable, IntWritable>() {
			@Override
			public void before() throws IOException {}

			@Override
			public void read(LongWritable pointId, IntWritable clusterId, PrintStream out) throws IOException {
				out.println("pointId: " + pointId.toString() + " clusterId: " + clusterId);
			}

			@Override
			public void after() throws IOException {}
		};
		
		// Write to text file
		SequenceFileReaderUtil.readPartFilesInDir(path, TEXT_FILE_MAX_ROWS, conf, new TextFileOutputReaderHandler<LongWritable, IntWritable>(path + "/pointsToClusters.txt", handler));
	}

	private static void readFinalClusters(Configuration conf, String path) throws IOException {
		readClusters(path, conf);
	}	
	
	private static void readClusteredPoints(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFilesInDirToConsole(path, 10, conf);
		
		// Write to text file
		SequenceFileReaderUtil.readPartFilesInDir(path, TEXT_FILE_MAX_ROWS, conf, new TextFileOutputReaderHandler<IntWritable, WeightedVectorWritable>(path + "/points.txt"));
	}

	// TODO May be there are clusters with words and their values
	// TODO read this file using clusterdump for better interpreting results
	private static void readInitialClusters(Configuration conf, String path) throws IOException {
		readClusters(path, conf);
	}

	private static void readClusters(String path, Configuration conf) throws IOException {
		ReaderHandler<IntWritable, ClusterWritable> handler = new ReaderHandler<IntWritable, ClusterWritable>() {
			@Override
			public void before() throws IOException {
			}

			@Override
			public void read(IntWritable key, ClusterWritable value, PrintStream out) throws IOException {
				out.println("Cluster id: " + key);
				out.println("Num objervations: " + value.getValue().getNumObservations());
				out.println("Total observations: " + value.getValue().getTotalObservations());
				out.println("Centroid: " + printVectorWithDictionary(value.getValue().getCenter()));
				out.println("");
			}

			@Override
			public void after() throws IOException {
			}
		};
		SequenceFileReaderUtil.readPartFilesInDir(path, 10, conf, new ConsoleReaderHandler<IntWritable, ClusterWritable>(handler));
		
		SequenceFileReaderUtil.readPartFilesInDir(path, TEXT_FILE_MAX_ROWS, conf, new TextFileOutputReaderHandler<IntWritable, ClusterWritable>(path + ".txt", handler));
	}
	
	// TODO remove code duplication with kmeaks cluster reading
	private static void readCanopyClusters(String path, Configuration conf) throws IOException {
		ReaderHandler<Text, ClusterWritable> handler = new ReaderHandler<Text, ClusterWritable>() {
			@Override
			public void before() throws IOException {
			}

			@Override
			public void read(Text key, ClusterWritable value, PrintStream out) throws IOException {
				out.println("Cluster id: " + key);
				out.println("Num objervations: " + value.getValue().getNumObservations());
				out.println("Total observations: " + value.getValue().getTotalObservations());
				out.println("Centroid: " + printVectorWithDictionary(value.getValue().getCenter()));
				out.println("");
			}

			@Override
			public void after() throws IOException {
			}
		};
		SequenceFileReaderUtil.readPartFilesInDir(path, 10, conf, new ConsoleReaderHandler<Text, ClusterWritable>(handler));
		
		SequenceFileReaderUtil.readPartFilesInDir(path, TEXT_FILE_MAX_ROWS, conf, new TextFileOutputReaderHandler<Text, ClusterWritable>(path + ".txt", handler));
	}	

	private static void readNGrams(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFilesInDirToConsole(path, 20, conf);
		SequenceFileReaderUtil.readPartFilesInDir(path, TEXT_FILE_MAX_ROWS, conf, new TextFileOutputReaderHandler(path + "/ngrams.txt"));
	}
	
	// This is a some statistics for n-grams with weight of each world in n-gram
	private static void readSubgrams(Configuration conf, String path) throws IOException {
		ReaderHandler<Gram, Gram> gramHandler = new ReaderHandler<Gram, Gram>() {
			@Override
			public void before() throws IOException {
			}

			@Override
			public void read(Gram key, Gram value, PrintStream out) throws IOException {
				out.println("gram: " + outGram(key) + "\t has gram: " + outGram(value));
			}

			@Override
			public void after() throws IOException {
			}
			
			public String outGram(Gram gram) {
				return gram.getString() + " [frq=" + gram.getFrequency() + ", type=" + gram.getType() + "]";
			}
		};
		
		// Output first 20 grams to console
		SequenceFileReaderUtil.readPartFilesInDir(path, 20, conf, new ConsoleReaderHandler<Gram, Gram>(gramHandler));
		
		// Print all grams to file
		SequenceFileReaderUtil.readPartFilesInDir(path, TEXT_FILE_MAX_ROWS, conf, new TextFileOutputReaderHandler<Gram, Gram>(path + "/subgrams.txt", gramHandler));		
	}	

	// TODO I do not understand why we need this file?
	private static void readDfCount(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFilesInDirToConsole(path, 20, conf);
	}

	private static void readTokenizedDocuments(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFilesInDirToConsole(path, DOCUMENTS_COUNT, conf);
	}

	// TODO which information does this frequency file contains?
	private static void readFrequencyFile(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFileToConsole(path, 20, conf);
	}

	private static String[] readDictionary(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFileToConsole(path, 20, conf);	
		
		// Output dictionary to text file
		ReaderHandler<Text, IntWritable> dictionaryToTextFileHandler = new TextFileOutputReaderHandler<Text, IntWritable>(BASE + "sparse/dictionary_text.txt");
		SequenceFileReaderUtil.readPartFile(path, TEXT_FILE_MAX_ROWS, conf, dictionaryToTextFileHandler);
		
		return VectorHelper.loadTermDictionary(conf, BASE + "sparse/dictionary.file-*");
	}	
	
	static SimpleConsoleReaderHandler<Text, VectorWritable> vectorHandler = new SimpleConsoleReaderHandler<Text, VectorWritable>() {
		@Override
		public void read(Text key, VectorWritable value, PrintStream myout) {
			System.out.println("Key: " + key);
			System.out.println("Vector: " + value.get().asFormatString());
			System.out.println("Vector + dictionary: " + printVectorWithDictionary(value.get()));
			// TODO What does the key represent?
		}
	};
	
	public static String printVectorWithDictionary(Vector vector) {
		StringBuilder result = new StringBuilder();
		for (Element element: vector.all()) {
			if (element.get() > 0) {
				result.append(dictionary[element.index()] + "[" + element.index() + "]:" + element.get() + ",");
			}
		}
		return result.toString();
	}
	
	private static void readTfVectors(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFilesInDir(path, DOCUMENTS_COUNT, conf, vectorHandler);
	}	
	
	private static void readTfIdfVectors(Configuration conf, String path) throws IOException {
		SequenceFileReaderUtil.readPartFilesInDir(path, DOCUMENTS_COUNT, conf, vectorHandler);
	}


	private static void readProcessedPostsToPostWritable(Configuration conf, String path) throws IOException {
		SimpleConsoleReaderHandler<LongWritable, PostWritable> handler = new SimpleConsoleReaderHandler<LongWritable, PostWritable>() {
			@Override
			public void read(LongWritable key, PostWritable value, PrintStream myout) {
				System.out.println("Post key: " + key);
				System.out.println("Post title: " + value.getTitle());
				System.out.println("Post content: " + value.getContent());
			}
		};
		
		SequenceFileReaderUtil.readPartFilesInDir(path, DOCUMENTS_COUNT, conf, handler);
	}
	
	private static void readProcessedPosts(Configuration conf, String path) throws IOException {
		SimpleConsoleReaderHandler<Text, Text> handler = new SimpleConsoleReaderHandler<Text, Text>() {
			@Override
			public void read(Text key, Text value, PrintStream myout) {
				System.out.println("Post key: " + key);
				System.out.println("Post value (title+text): " + value);
			}
		};
		
		SequenceFileReaderUtil.readPartFilesInDir(path, DOCUMENTS_COUNT, conf, handler);
	}	
}