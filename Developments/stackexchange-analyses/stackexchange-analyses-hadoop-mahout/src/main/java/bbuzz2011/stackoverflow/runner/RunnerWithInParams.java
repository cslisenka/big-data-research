package bbuzz2011.stackoverflow.runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import bbuzz2011.stackoverflow.StackOverflowAnalyzer;
import bbuzz2011.stackoverflow.join.ClusterJoinerJob;
import bbuzz2011.stackoverflow.join.PointToClusterMappingJob;
import bbuzz2011.stackoverflow.preprocess.text.StackOverflowPostTextExtracterJob;
import bbuzz2011.stackoverflow.preprocess.xml.StackOverflowPostXMLParserJob;

/**
 * Runs clustering job:
 * 1. Prepare data
 * 2. Vectorize
 * 3. Run Clustering
 * 4. Post process
 * 5. Send to Lucene 
 */
public class RunnerWithInParams {

    private Configuration configuration = new Configuration();
    private Path outputBasePath;// = new Path("target/stackoverflow-output-base/");

    private String outputDictionaryPattern;
    private Path outputPostsPath;
    private Path outputSeq2SparsePath;
    private Path outputVectorPath;

    /**
     * Args
     * 0 = input (amazon s3)
     * 1 = output (amazon s3)
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        RunnerWithInParams runner = new RunnerWithInParams();
        runner.run(args);
    }

    private void run(String[] args) throws Exception {
    	outputBasePath = new Path(args[1]);
        outputSeq2SparsePath = new Path(outputBasePath, "sparse");
        outputVectorPath = new Path(outputSeq2SparsePath, "tfidf-vectors");
        outputDictionaryPattern = new Path(outputSeq2SparsePath, "dictionary.file-*").toString();       	
//        cleanOutputBasePath();
        preProcess(args[0]);
        vectorize();
        cluster();
        postProcess(new Path(outputBasePath, "kmeans"), "kmeans");
    }

//    private void cleanOutputBasePath() throws IOException {
//        HadoopUtil.delete(configuration, outputBasePath);
//    }

    private void preProcess(String input) throws ClassNotFoundException, IOException, InterruptedException {
    	// TODO probably specify directory
        configuration.set(StackOverflowPostXMLParserJob.INPUT, input);
        configuration.set(StackOverflowPostXMLParserJob.OUTPUT, outputBasePath.toString());

        
        // Parse posts.xml to sequence file [PostId] [PostWritable(Title, Text)]
        // We need this information later after clustering finish. We'll join it with clustered IDs.
        StackOverflowPostXMLParserJob parseJob = new StackOverflowPostXMLParserJob(configuration);
        outputPostsPath = parseJob.parseXML();

        configuration.set(StackOverflowPostTextExtracterJob.INPUT, new Path(outputBasePath, StackOverflowPostXMLParserJob.OUTPUT_POSTS_PATH).toString());
        configuration.set(StackOverflowPostTextExtracterJob.OUTPUT, outputBasePath.toString());

        // Parse sequence file [PostId] [PostWritable(Title, Text)] to sequence file [PostId] [Title+Text] format
        // Result of this job will go to vectorizing and clustering job
        StackOverflowPostTextExtracterJob extracterJob = new StackOverflowPostTextExtracterJob(configuration);
        extracterJob.run();
    }

    /**
     * Convert sequence file with posts to vectors
     * @throws Exception
     */
    private void vectorize() throws Exception {
        String[] seq2SparseArgs = new String[] {
                "--input", new Path(outputBasePath, StackOverflowPostTextExtracterJob.OUTPUT_POSTS_TEXT).toString(),
                "--output", outputSeq2SparsePath.toString(),
                // Maximum size of word groups, which are often together and represent one item (Coca Cola, Unit Testing, Continuous Integration)
                // 2 for better clustering and not very large calculation
                "--maxNGramSize", "2",
                "--namedVector",
                // Maximum document frequency percentage. All terms grater this value will be kicked.
                // Also minimum document frequency could be specified.
                "--maxDFPercent", "25",
                "--norm", "2",
                // Plug-in our Apache Lucene analyzer for filtering content (removing stop-words, etc.)
                "--analyzerName", StackOverflowAnalyzer.class.getName(),
                "--overwrite"
        };

        ToolRunner.run(configuration, new SparseVectorsFromSequenceFiles(), seq2SparseArgs);
    }

    private void cluster() throws Exception {
        Path outputKMeansPath = new Path(outputBasePath, "kmeans");
        Path clustersPath = new Path(outputKMeansPath, "stackoverflow-kmeans-initial-clusters");

        // TODO where are initial clusters generated?
        String[] kmeansDriver = {
                "--input", outputVectorPath.toString(),
                "--output", outputKMeansPath.toString(),
                "--clusters", clustersPath.toString(),
                // Max iterations number
                "--maxIter", "25",
                // Make 60 initial clusters
                "--numClusters", "60",
                "--distanceMeasure", CosineDistanceMeasure.class.getName(),
                "--clustering",
                "--method", "sequential",
                "--overwrite"
        };

        KMeansDriver kmeans = new KMeansDriver();
        ToolRunner.run(configuration, kmeans, kmeansDriver);        
    }

    private void postProcess(Path outputClusteringPath, String algorithmId) throws Exception {
        Path clusteredPointsPath = new Path(outputClusteringPath, "clusteredPoints");
        Path outputFinalClustersPath = new Path(outputClusteringPath, "clusters-*-final/*");
        Path pointsToClusterPath = new Path(outputBasePath, "pointsToClusters_" + algorithmId);
        // TODO I detected bug here. In original source Path clusteredPostsPath = new Path(outputBasePath, "clusteredPosts"); 
        Path clusteredPostsPath = new Path(outputBasePath, "clusteredPosts_" + algorithmId);

        PointToClusterMappingJob pointsToClusterMappingJob = new PointToClusterMappingJob(clusteredPointsPath, pointsToClusterPath);
        pointsToClusterMappingJob.setConf(configuration);
        pointsToClusterMappingJob.mapPointsToClusters();

        ClusterJoinerJob clusterJoinerJob = new ClusterJoinerJob(outputPostsPath, pointsToClusterPath, clusteredPostsPath);
        clusterJoinerJob.setConf(configuration);
        clusterJoinerJob.run();
    }
}