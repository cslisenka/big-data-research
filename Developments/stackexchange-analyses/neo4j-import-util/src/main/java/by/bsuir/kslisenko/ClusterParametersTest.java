package by.bsuir.kslisenko;

import org.apache.mahout.clustering.streaming.tools.ClusterQualitySummarizer;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class ClusterParametersTest {

	private static final String BASE = "../stackexchange-analyses-hadoop-mahout/target/stackoverflow-output-base/";
	
	public static void main(String[] args) throws Exception {
//		runDumper();
		ClusterQualitySummarizer qa = new ClusterQualitySummarizer();
		qa.run(new String[] {
			"--input", BASE + "kmeans/clusters-1-final",
			"--output", "ClusterQualitySummarizer.txt",
			"--centroids", BASE + "kmeans/clusters-1-final",
			"--mahoutkmeansformat", "true"
		});
	}

	private static void runDumper() throws Exception {
		ClusterDumper dumper = new ClusterDumper();
		dumper.run(new String[] {
				"--outputFormat", "TEXT",
				"--numWords", "5",
				"--pointsDir", BASE + "kmeans/clusteredPoints",
				"--input", BASE + "kmeans/clusters-1-final",
				"--output", "cluster-dumper.txt",
				"--samplePoints", "10",
				"--dictionary", BASE + "sparse/dictionary.file-0",
				"--dictionaryType", "sequencefile",
				"--distanceMeasure", CosineDistanceMeasure.class.getName(),
				"--evaluate"
		});
	}
}