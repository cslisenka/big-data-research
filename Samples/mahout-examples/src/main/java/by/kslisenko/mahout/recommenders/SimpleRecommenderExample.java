package by.kslisenko.mahout.recommenders;

import java.io.File;
import java.util.List;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * Example of simple user-based recommender.
 * Mahout doesn't represent one recomender. It contains numerous
 * of plug-in components. Developer can variate this components
 * to build the best recommender for specified domain area.
 * 
 * @author kslisenko
 */
public class SimpleRecommenderExample {

	public static void main(String[] args) throws Exception {
		// Implementation stores, provides access to all the preference, user, and item data needed in the computation
		DataModel model = new FileDataModel(new File(new File("").getAbsoluteFile(), "src/main/resources/recommender/intro.csv"));
		
		// Notion of how similar two users are
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		
		// Notion of a group of users that are most similar to a given user
		// This neighborhood contains of 2 users
		// It's also possible to define theshold instead of concrete number of users (if we do not know how many users might be neighbors)
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model);
		
		Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
		
		int userId = 1;
		int recommendationsCount = 2;
		List<RecommendedItem> recommendations = recommender.recommend(userId, recommendationsCount);
		
		for (RecommendedItem recommendation : recommendations) {
			System.out.println(recommendation);
		}
	}
}