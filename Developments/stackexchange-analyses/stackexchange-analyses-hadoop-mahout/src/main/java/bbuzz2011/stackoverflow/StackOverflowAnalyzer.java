package bbuzz2011.stackoverflow;

import java.io.Reader;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Analyzer which plug-in to mahout TF-IDF vector generation utility
 */
public class StackOverflowAnalyzer extends Analyzer {

	final List<String> stopWords = Arrays.asList("what", "where", "how",
			"when", "why", "which", "were", "find", "myself", "these", "know",
			"anybody", "somebody", "differences", "good", "best", "much",
			"less", "more", "most", "been", "reading", "your", "mine", "with",
			"doing", "interested", "also", "from", "that", "like", "there",
			"would", "answer", "question", "need", "about", "have", "this",
			"using", "another", "difference", "between", "across", "want", "able", "although", "always");

	@Override
	protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
		LetterTokenizer tokenizer = new LetterTokenizer(Version.LUCENE_43, reader);
		
		TokenStream tok = new LowerCaseFilter(Version.LUCENE_43, tokenizer);
		tok = new StandardFilter(Version.LUCENE_43, tok);
		tok = new LengthFilter(true, tok, 4, 15);
//		tok = new LengthFilter(Version.LUCENE_43, tok, 4, 15);
		tok = new PorterStemFilter(tok);
		
		final CharArraySet stopSet = new CharArraySet(Version.LUCENE_43, stopWords.size(), true);
		stopSet.addAll(stopWords);
		stopSet.add(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
		tok = new StopFilter(Version.LUCENE_43, tok, stopSet);
		
		return new TokenStreamComponents(tokenizer, tok);
	}
}