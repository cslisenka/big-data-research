package by.kslisenko.mahout.recommenders;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;

/**
 * Testing recommender example. This test prints several parameters of recommender.
 * It could be used for choosing better recommender for our data.
 * 
 * @author cloudera
 * 
 */
public class ScoreSimpleRecommender {

	public static void main(String[] args) throws IOException, TasteException {
		// Forces the same random choices each time.
		// Training and test datasets may differ at each run
		RandomUtils.useTestSeed();
		DataModel model = new FileDataModel(new File(
				new File("").getAbsoluteFile(),
				"src/main/resources/recommender/intro.csv"));

		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		RecommenderBuilder builder = new RecommenderBuilder() {
			public Recommender buildRecommender(DataModel model)
					throws TasteException {
				UserSimilarity similarity = new PearsonCorrelationSimilarity(
						model);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(2,
						similarity, model);
				return new GenericUserBasedRecommender(model, neighborhood,
						similarity);
			}
		};

		// The RecommenderEvaluator splits the data into a training and test
		// set,
		// builds a new training DataModel and Recommender to test,
		// and compares its estimated preferences to the actual test data.

		// Train recommender with 70% of data; test with 30%.
		// 1.0 = using 100% of data. It is good for quick testing recommender on
		// data subsets
		double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);

		// Score indicating how well the Recommender performed
		System.out.println(score);

		// A result of 1.0 from this implementation means that, on average, the
		// recommender estimates a preference that
		// deviates from the actual preference by 1.0.
		RecommenderIRStatsEvaluator evaluatorIRStats = new GenericRecommenderIRStatsEvaluator ();
		IRStatistics stats = evaluatorIRStats.evaluate(builder, null, model, null, 2,
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		// Percent of good recommendations
		System.out.println(stats.getPrecision());
		
		// Percent of good recommendations appear in results
		System.out.println(stats.getRecall());

	}
}