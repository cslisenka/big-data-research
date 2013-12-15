package bbuzz2011.stackoverflow.runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

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
public class Runner {

    private Configuration configuration = new Configuration();
    private Path outputBasePath = new Path("target/stackoverflow-output-base/");

    private String outputDictionaryPattern;
    private Path outputPostsPath;
    private Path outputSeq2SparsePath;
    private Path outputVectorPath;

    public static void main(String[] args) throws Exception {
        Runner runner = new Runner();
        runner.run();
    }

    private void run() throws Exception {
        outputSeq2SparsePath = new Path(outputBasePath, "sparse");
        outputVectorPath = new Path(outputSeq2SparsePath, "tfidf-vectors");
        outputDictionaryPattern = new Path(outputSeq2SparsePath, "dictionary.file-*").toString();       	
        cleanOutputBasePath();
        preProcess();
        vectorize();
        cluster();
        postProcess();
    }

    private void cleanOutputBasePath() throws IOException {
        HadoopUtil.delete(configuration, outputBasePath);
    }

    private void preProcess() throws ClassNotFoundException, IOException, InterruptedException {
        configuration.set(StackOverflowPostXMLParserJob.INPUT, "src/main/resources/posts-real.xml");
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
        String[] seq2SparseArgs = new String[]{
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
                "--analyzerName", "bbuzz2011.stackoverflow.StackOverflowAnalyzer",
                "--overwrite"
        };

        ToolRunner.run(configuration, new SparseVectorsFromSequenceFiles(), seq2SparseArgs);
    }

    private void cluster() throws Exception {
        Path outputKMeansPath = new Path(outputBasePath, "kmeans");

        // TODO where are initial clusters generated?
        String[] kmeansDriver = {
                "--input", outputVectorPath.toString(),
                "--output", outputKMeansPath.toString(),
                "--clusters", "target/stackoverflow-kmeans-initial-clusters",
                // Max iterations number
                "--maxIter", "10",
                // Make 250 initial clusters
                "--numClusters", "250",
                "--distanceMeasure", CosineDistanceMeasure.class.getName(),
                // TODO may be run input vector clustering after computing Canopies?
                "--clustering",
                "--method", "sequential",
                "--overwrite"
        };

        ToolRunner.run(configuration, new KMeansDriver(), kmeansDriver);

//        Path outputCanopy = new Path(outputBasePath, "canopy");        
//        
//        // TODO where are initial clusters generated?
//        String[] canopyDriver = {
//                "--input", outputVectorPath.toString(),
//                "--output", outputCanopy.toString(),
//                "--distanceMeasure", CosineDistanceMeasure.class.getName(),
//                "--t1", "1500",
//                "--t2", "2500",
//                "--clusterFilter", "0"
//        };        
//        
//        ToolRunner.run(configuration, new CanopyDriver(), canopyDriver);
        
        Path outputFuzzyKMeans = new Path(outputBasePath, "fuzzy-kmeans");        
        
        // TODO where are initial clusters generated?
        String[] fuzzyKMeansDriver = {
                "--input", outputVectorPath.toString(),
                "--output", outputFuzzyKMeans.toString(),
                "--distanceMeasure", CosineDistanceMeasure.class.getName(),
                // Fuzziest
                "--m", "3",
				// Max iterations number
				"--maxIter", "10",
				//"-e", "false",
				//"-t", "0",
				"--method", "mapreduce",
				"--clusters", "target/stackoverflow-fuzzy-kmeans-initial-clusters",
				"--overwrite",
				"--clustering",
				"--numClusters", "250"
        };        
        
//        ToolRunner.run(configuration, new FuzzyKMeansDriver(), fuzzyKMeansDriver);        
    }

    private void postProcess() throws Exception {
        Path outputClusteringPath = new Path(outputBasePath, "kmeans");
        Path clusteredPointsPath = new Path(outputClusteringPath, "clusteredPoints");
        Path outputFinalClustersPath = new Path(outputClusteringPath, "clusters-*-final/*");
        Path pointsToClusterPath = new Path(outputBasePath, "pointsToClusters");
        // TODO I detected bug here. In original source Path clusteredPostsPath = new Path(outputBasePath, "clusteredPosts"); 
        Path clusteredPostsPath = new Path(outputBasePath, "clusteredPosts");

        PointToClusterMappingJob pointsToClusterMappingJob = new PointToClusterMappingJob(clusteredPointsPath, pointsToClusterPath);
        pointsToClusterMappingJob.setConf(configuration);
        pointsToClusterMappingJob.mapPointsToClusters();

        ClusterJoinerJob clusterJoinerJob = new ClusterJoinerJob(outputPostsPath, pointsToClusterPath, clusteredPostsPath);
        clusterJoinerJob.setConf(configuration);
        clusterJoinerJob.run();
    }
}
