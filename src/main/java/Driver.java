import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import scala.Tuple2;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.store.RAMDirectory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Driver {
	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf();
		 
		sparkConf.setAppName("boolean search");
		//sparkConf.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		System.out.println("searchString:"+args[0]);
		startSearch(sc, args[0]);
		sc.close();
		
	}

	public static String getKeyWords(String searchWords){
		String[] words = toKeyWordsArray(searchWords);
		StringBuilder result = new StringBuilder();
		int index = 0;
		while(index < words.length - 1) {
			if(words[index].equals("and not")) {
				if(words[index + 1].equals("(")) {
					while(!words[++index].equals(")"));
				}
				else {
					index++;
				}
			}
			else if(!words[index].equals("(")&&!words[index].equals(")")&&!words[index].equals("and")&&!words[index].equals("or")) {
				result.append(words[index]);
				result.append(" ");
			}
			index++;
		}
		return result.toString();
	}
	
	
	public static JavaRDD<String> booleanSearch(HashMap<String,JavaRDD<String>> mapWordToDocID,String searchString) {
		String[] words = toKeyWordsArray(searchString);
		Stack<JavaRDD<String>> operands = new Stack();
		Stack<String> operators = new Stack();
		
		for(String word: words) {
			if(word.equals("(")) {
				operators.push(word);
			}
			else if(word.equals("and")||word.equals("or")||word.equals("and not")||word.equals(")")) {
				if(!operators.isEmpty()&&!operators.peek().equals("(")) {
					operands.push(calculator(operands.pop(),operands.pop(),operators.pop()));
				}
				if(word.equals(")")) {
					operators.pop();
				}
				else {
					operators.push(word);
				}
			}
			else if(word.equals("#")) {
				while(!operators.isEmpty()) {
					operands.push(calculator(operands.pop(),operands.pop(),operators.pop()));
				}
			}
			else {
				operands.push(mapWordToDocID.get(word));
			}
			
		}
		return operands.pop();

	}
	
	public static JavaRDD<String> calculator(JavaRDD<String> operand1,JavaRDD<String> operand2, String operator){
		JavaRDD<String> result = null;
		switch(operator) {
		case "and":
			result = operand2.intersection(operand2);
			break;
		case "or":
			result = operand2.union(operand1);
			break;
		case "and not":
			result = operand2.subtract(operand1);
			break;
		default:
		}
		return result;
	}
	public static String[] toKeyWordsArray(String searchWords) {
		searchWords = searchWords.toLowerCase();
		ArrayList<String> result = new ArrayList();
		for(int i = 0; i < searchWords.length(); i++) {
			char c = searchWords.charAt(i);
			if(Character.isLetter(c)) {
				int j = i;
				while(++j < searchWords.length()) {
					if(!Character.isLetter(searchWords.charAt(j))) {
						break;
					}
				}
				String newWord = searchWords.substring(i,j);
				if(newWord.equals("not")) {
					result.remove(result.size()-1);
					result.add("and not");
				}
				else {
					result.add(newWord);
				}
				i = j - 1;
			}
			else if(c == '(') {
				result.add("(");
			}
			else if(c == ')') {
				result.add(")");
			}
		}
		result.add("#");
		return result.toArray(new String[result.size()]);
	}

	public static String webInfo(String line, String keywords) {
		String[] docline = line.split(",");
		String docID = docline[0];
		String docURL = docline[1];
		String title = docline[2];
		String content = docline[3];

		String titleSnippet = textSnippet(title, keywords);
		String contentSnippet = textSnippet(content, keywords);

		String info[] = new String[4];
//		info[0] = docID;
//		info[1] = docURL;
//		info[2] = titleSnippet;
//		info[3] = contentSnippet;
		return docID+" | "+docURL+" | "+titleSnippet+" | "+contentSnippet;

	}


	public static String textSnippet(String content, String keywords) {
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		RAMDirectory ramDirectory = new RAMDirectory();
		IndexWriter indexWriter;
		Document doc = new Document(); // create a new document

		/**
		 * Create a field with term vector enabled
		 */
		FieldType type = new FieldType();
		type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		type.setStored(true);
		type.setStoreTermVectors(true);
		type.setTokenized(true);
		type.setStoreTermVectorOffsets(true);

		Field f = new Field("content", content, type);
		doc.add(f);

		try {
			indexWriter = new IndexWriter(ramDirectory, config);
			indexWriter.addDocument(doc);
			indexWriter.close();

			IndexReader idxReader = DirectoryReader.open(ramDirectory);
			IndexSearcher idxSearcher = new IndexSearcher(idxReader);
			Query queryToSearch = new QueryParser("content", analyzer).parse(keywords);
			TopDocs hits = idxSearcher
					.search(queryToSearch, idxReader.maxDoc());
			SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter();
			Highlighter highlighter = new Highlighter(htmlFormatter,
					new QueryScorer(queryToSearch));

//			System.out.println("reader maxDoc is " + idxReader.maxDoc());
//			System.out.println("scoreDoc size: " + hits.scoreDocs.length);
			for (int i = 0; i < hits.totalHits; i++) {
				int id = hits.scoreDocs[i].doc;
				Document docHit = idxSearcher.doc(id);
				String text = docHit.get("content");
				TokenStream tokenStream = TokenSources.getAnyTokenStream(idxReader, id, "content", analyzer);
				TextFragment[] frag = highlighter.getBestTextFragments(tokenStream, text, false, 4);
				for (int j = 0; j < frag.length; j++) {
					if ((frag[j] != null) && (frag[j].getScore() > 0)) {
						//System.out.println((frag[j].toString()));
						return frag[j].toString();
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTokenOffsetsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	/**
	 * 
	 * @param searchString
	 * @return all keywords in the given search String
	 */
	public static List<String> getSearchWords(String searchString){
		String[] words = toKeyWordsArray(searchString);
		List<String> result = new ArrayList();
		for(String s : words) {
			if(!s.equals("(")&&!s.equals(")")&&!s.equals("and")&&!s.equals("or")&&!s.equals("and not")&&!s.equals("#")) {
				result.add(s);
			}
		}
		return result;
	}
	

	
	public static HashMap<String,HashSet<String>> getInitialsMap(List<String> searchWords){
		Iterator<String> it = searchWords.iterator();
		HashMap<String,HashSet<String>> result = new HashMap();
		while(it.hasNext()) {
			String currWord = it.next();
			String initial = currWord.substring(0, 2).toLowerCase();
			if(!result.containsKey(initial)) {
				result.put(initial, new HashSet<String>(Arrays.asList(currWord)));
			}
			else {
				result.get(initial).add(currWord);
			}
		}
		return result;
	}
	/**
	 * 
	 * @param sc
	 * @param invertedIndexPwd
	 * @param keyWords
	 * @return the RDD contains all lines of keywords with given initial file
	 */
	public static JavaRDD<String> getRDDofInitial(JavaSparkContext sc, String invertedIndexPwd, HashSet<String> keyWords){
		JavaRDD<String> invertedIndexRDD = sc.textFile(invertedIndexPwd);
		JavaRDD<String> result = invertedIndexRDD.filter(s -> {
			String[] tmp = s.split("\\s+");
            return (keyWords.contains(tmp[0]));
		}).cache();
		return result;
	}
	
	public static void buildHashMap(JavaSparkContext sc, HashMap<String,JavaRDD<String>> mapWordToDocID, HashMap<String,List<String>> mapDocIDToWord, JavaRDD<String> initialRDD) {
		initialRDD.collect().forEach(s -> {
			String[] line = s.split("\\s+");
			String word = line[0];
			List<String> docIDList = new ArrayList(Arrays.asList(line));
			docIDList.remove(0);
			mapWordToDocID.put(word, sc.parallelize(docIDList));
			Iterator<String> it = docIDList.iterator();
			while(it.hasNext()) {
				String docID = it.next();
				if(!mapDocIDToWord.containsKey(docID)) {
					mapDocIDToWord.put(docID, new ArrayList<String>(Arrays.asList(word)));
				}
				else {
					mapDocIDToWord.get(docID).add(word);
				}
			}
		});
	}
	
	public static String getWikiFilesPwd(List<String> fileNames) {
		StringBuilder sb = new StringBuilder();
		Iterator<String> it = fileNames.iterator();
		while(it.hasNext()) {
			sb.append("/data/wiki_csv/"+it.next());
			if(it.hasNext()) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
	/*
	public static List<String> getWikiFileNames(List<String> docID) {
		String csvFile = "/class/cs132/wiki_ranges.csv";
		//String csvFile = "data/wiki_ranges.csv";
        BufferedReader br = null;
        String line = "", result = "";
        Iterator<String> it = docID.iterator();
        List<Integer> docIDInt = new ArrayList();
        List<String> fileNames = new ArrayList();
        while(it.hasNext()) {
        		docIDInt.add(Integer.parseInt(it.next()));
        }
        Collections.sort(docIDInt);
        try {
            br = new BufferedReader(new FileReader(csvFile));
            int index = 0;
            while ((line = br.readLine()) != null && index < docIDInt.size()) {
                String[] range = line.split(",");
                int begin = Integer.parseInt(range[1]);
	    			int end = Integer.parseInt(range[2]);
	    			int docId = docIDInt.get(index);
	    			while(docId < begin && index < docIDInt.size() - 1) {
	    				docId = docIDInt.get(++index);
	    			}
	    			if(docId >= begin && docId <= end) {
	    				fileNames.add(range[0]);
	    				index++;
	    			}
            }
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return fileNames;
	}
	*/
	public static List<String> getWikiFileNames(JavaSparkContext sc, List<String> docID) {
		wikiFileNameAccumulator myAccumulator = new wikiFileNameAccumulator();
		sc.sc().register(myAccumulator,"wikiFileNameAccumulator");
		JavaRDD<String> rangesRDD = sc.textFile("/user/cs132g2/wiki_ranges.csv");
		//String csvFile = "/class/cs132/wiki_ranges.csv";
		//String csvFile = "data/wiki_ranges.csv";
		rangesRDD.foreach(line -> {
			String[] range = line.split(",");
            int begin = Integer.parseInt(range[1]);
    			int end = Integer.parseInt(range[2]);
    			Iterator<String> iterator = docID.iterator();
    			while(iterator.hasNext()) {
    				int docId = Integer.parseInt(iterator.next());
        			if(docId >= begin && docId <= end) {
        				myAccumulator.add(range[0]);
        			}
    			}
		});
		return new ArrayList<String>(myAccumulator.value());
	}
	
	public static List<String> getDocIDList(JavaRDD<String> docIDRDD){
		List<String> result = new ArrayList();
		docIDRDD.collect().forEach(s -> {
			result.add(s);
		});
		return result;
	}
	
	public static void startSearch(JavaSparkContext sc, String searchString) {
		
		//String invertedIndexDir = "data/";
		String invertedIndexDir = "/user/cs132g2/splitted_inverted_index_double/";
		HashMap<String,HashSet<String>> initialsMap = getInitialsMap(getSearchWords(searchString));
		HashMap<String,JavaRDD<String>> mapWordToDocID = new HashMap();
		HashMap<String,List<String>> mapDocIDToWord = new HashMap();
		Iterator<Map.Entry<String, HashSet<String>>> it = initialsMap.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String, HashSet<String>> entry = it.next();
			JavaRDD<String> initialRDD = getRDDofInitial(sc, invertedIndexDir+"invertedindex_"+entry.getKey(), entry.getValue());
			buildHashMap(sc, mapWordToDocID, mapDocIDToWord, initialRDD);
		}
		JavaRDD<String> resultDocIDRDD = booleanSearch(mapWordToDocID,searchString).cache();
		
		JavaPairRDD<String,String> articles = sc.textFile(getWikiFilesPwd(getWikiFileNames(sc,getDocIDList(resultDocIDRDD)))).mapToPair(tuple -> {
			String docline[] = tuple.split(",");
			return new Tuple2<String,String>(docline[0], tuple);
		});
		List<String[]> res = new ArrayList<>();//web infos
		List<String> ids = new ArrayList<>();

		//JavaPairRDD<String, String> invertedIndex = booleanSearch(invertedIndexFile,line);
		//invertedIndex.saveAsTextFile("hdfs:///user/cs132g2/output_wiki_search");
		
			JavaPairRDD<String, Tuple2<String, String>> searchedArticles = articles.join(resultDocIDRDD.mapToPair(s -> {
				return new Tuple2<String,String>(s,"");
			}));

			JavaRDD<String> resRDD = searchedArticles.map(tuple -> {
				String content = tuple._2._1;
				StringBuilder sb = new StringBuilder();
				List<String> keyWordsList = mapDocIDToWord.get(tuple._1);
				Iterator<String> iterator = keyWordsList.iterator();
				while(iterator.hasNext()) {
					sb.append(iterator.next());
					if(iterator.hasNext()) {
						sb.append(" ");
					}
				}
				String info = webInfo(content, sb.toString());

				return info;
			});
			resRDD.saveAsTextFile("/user/cs132g2/output_wiki_search");
	}
}

class wikiFileNameAccumulator extends AccumulatorV2<String, HashSet<String>> {  
	private HashSet<String> fileNameSet;
	public wikiFileNameAccumulator() {
		fileNameSet = new HashSet();
	}
	public wikiFileNameAccumulator(HashSet<String> set) {
		fileNameSet = new HashSet(set);
	}
	@Override
	public void reset() {
		fileNameSet = new HashSet();
	}
	
	@Override
	public void add(String s) {
		fileNameSet.add(s);
	}

	@Override
	public AccumulatorV2<String, HashSet<String>> copy() {
		// TODO Auto-generated method stub
		return new wikiFileNameAccumulator(fileNameSet);
	}

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
		return fileNameSet.isEmpty();
	}

	@Override
	public void merge(AccumulatorV2<String, HashSet<String>> arg0) {
		// TODO Auto-generated method stub
		fileNameSet.addAll(arg0.value());
	}

	@Override
	public HashSet<String> value() {
		// TODO Auto-generated method stub
		return new HashSet<String>(fileNameSet);
	}
}   

