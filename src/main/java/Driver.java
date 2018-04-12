import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Stack;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Driver {
	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf();
		 
		sparkConf.setAppName("boolean search");
		sparkConf.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> invertedIndexFile = sc.textFile("output/part-r-00000").cache();
	    
		Scanner scanner = new Scanner(System.in);
		String line;
		while (!(line = scanner.nextLine()).equals("q")) {
			booleanSearch(invertedIndexFile,line);
		}

		sc.close();
	}
	
	
	public static void booleanSearch(JavaRDD<String> invertedIndexRDD,String searchWords) {
		String[] words = toKeyWordsArray(searchWords);
		Stack<JavaPairRDD<String,String>> operands = new Stack();
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
				operands.push(getPairRDD(invertedIndexRDD,word));
			}
			
		}
		System.out.println("finished");
		JavaPairRDD<String,Iterable<String>> tmp = operands.pop().groupByKey();
		JavaPairRDD<String,String> result = tmp.mapToPair(tuple -> {
			StringBuilder sb = new StringBuilder();
			Iterator<String> it = tuple._2.iterator();
			while(it.hasNext()) {
				sb.append(it.next()+" ");
			}
			return new Tuple2<String,String>(tuple._1,sb.toString().substring(0,sb.length()-1));
		});
		result.foreach(data -> {
	        System.out.println("docID="+data._1() + " position=" + data._2());
	    }); 
		
	}
	
	public static JavaPairRDD<String,String> calculator(JavaPairRDD<String,String> operand1,JavaPairRDD<String,String> operand2, String operator){
		JavaPairRDD<String,String> result = null;
		switch(operator) {
		case "and":
			result = operand2.cogroup(operand1).filter(tuple -> tuple._2._1.iterator().hasNext()&&tuple._2._2.iterator().hasNext())
			.mapToPair(tuple -> {
				StringBuilder sb = new StringBuilder();
				Iterator<String> it = tuple._2._1.iterator();
				while(it.hasNext()) {
					sb.append(it.next()+" ");
				}
				it = tuple._2._2.iterator();
				while(it.hasNext()) {
					sb.append(it.next()+" ");
				}
				return new Tuple2<String,String>(tuple._1,sb.toString().substring(0,sb.length()-1));
			});
			/*new Function<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>,Boolean>() {
				@Override  
	            public Boolean call(Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>> s) throws Exception {  
	            		return s._2._1.iterator().hasNext()&&s._2._2.iterator().hasNext();
	            }
			}*/
			break;
		case "or":
			result = operand2.union(operand1);
			break;
		case "and not":
			result = operand2.subtractByKey(operand1);
			break;
		default:
		}
		return result;
	}
	
	public static JavaPairRDD<String,String> getPairRDD(JavaRDD<String> invertedIndexRDD,String keyWord){
		JavaRDD<String> wordLine = invertedIndexRDD.filter(s -> {
			String[] tmp = s.split("\\s+");
            return (tmp[0].equals(keyWord));
		}); 
		/*
		 * new Function<String, Boolean>() {  
            @Override  
            public Boolean call(String s) throws Exception {  
            		String[] tmp = s.split("\\s+");
               return (tmp[0].equals(keyWord));  
            }  
        }*/
		JavaPairRDD<String,String> pairRDD = wordLine.flatMapToPair(s ->{
				ArrayList<Tuple2<String, String>> result = new ArrayList();
            		for(int i = 0; i < s.length(); i++) {
            			if(s.charAt(i) == '(') {
            				int j = i;
            				String docId = "", position = "";
            				while(s.charAt(++j) != ')') {
            					if(s.charAt(j) == 'd') {
            						int k = j+1;
            						while(Character.isDigit(s.charAt(++k)));
            						docId = s.substring(j+2, k);
            						j = k;
            					}
            					else if(s.charAt(j) == 'p') {
            						int k = j+1;
            						while(Character.isDigit(s.charAt(++k)));
            						position = s.substring(j+2, k);
            						j = k - 1;
            					}
            				}
            				result.add(new Tuple2<String,String>(docId,position));
            			}
            		}
            		return result.iterator(); 
		});
		return pairRDD;
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
}

/*
 * new PairFlatMapFunction<String,String,String>(){
			@Override  
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {  
				ArrayList<Tuple2<String, String>> result = new ArrayList();
            		for(int i = 0; i < s.length(); i++) {
            			if(s.charAt(i) == '(') {
            				int j = i;
            				String docId = "", position = "";
            				while(s.charAt(++j) != ')') {
            					if(s.charAt(j) == 'd') {
            						int k = j+1;
            						while(Character.isDigit(s.charAt(++k)));
            						docId = s.substring(j+2, k);
            						j = k;
            					}
            					else if(s.charAt(j) == 'p') {
            						int k = j+1;
            						while(Character.isDigit(s.charAt(++k)));
            						position = s.substring(j+2, k);
            						j = k - 1;
            					}
            				}
            				result.add(new Tuple2<String,String>(docId,position));
            			}
            		}
            		return result.iterator();
            } 
		}*/
