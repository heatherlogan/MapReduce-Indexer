
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.net.URI;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.PorterStemmer;


public class MyIndexer extends Configured implements Tool {
	
	
	static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		private fin
		import java.io.BufferedReader;
		import java.io.File;
		import java.io.FileReader;
		import java.io.IOException;
		import java.util.ArrayList;
		import java.util.Collection;
		import java.util.Collections;
		import java.util.Comparator;
		import java.util.HashMap;
		import java.util.HashSet;
		import java.util.LinkedHashMap;
		import java.util.List;
		import java.net.URI;
		import java.util.stream.Collectors;
		import java.util.stream.Stream;
		import java.util.stream.StreamSupport;
		import java.util.Set;
		import java.util.StringTokenizer;
		import org.apache.hadoop.util.StringUtils;
		import org.apache.hadoop.conf.Configuration;
		import org.apache.hadoop.conf.Configured;
		import org.apache.hadoop.fs.Path;
		import org.apache.hadoop.io.GenericWritable;
		import org.apache.hadoop.io.IntWritable;
		import org.apache.hadoop.io.LongWritable;
		import org.apache.hadoop.io.Text;
		import org.apache.hadoop.mapreduce.Job;
		import org.apache.hadoop.mapreduce.Mapper;
		import org.apache.hadoop.mapreduce.Reducer;
		import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
		import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
		import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
		import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
		import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
		import org.apache.hadoop.mapreduce.lib.input.FileSplit;
		import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
		import org.apache.hadoop.mapreduce.Counter;
		import org.apache.hadoop.mapreduce.Counters;
		import org.apache.hadoop.util.Tool;
		import org.apache.hadoop.util.ToolRunner;

		import com.google.common.collect.Lists;

		import utils.PorterStemmer;

		public class MyIndexer extends Configured implements Tool {
			
			
			static class Map extends Mapper<LongWritable, Text, Text, IndexWritable> {
				
				private final static IntWritable one = new IntWritable(1);		
				public static final String Entry = null;
				private Text word = new Text();
				private Set<String> stopWordList = new HashSet<String>();
				PorterStemmer stemmer = new PorterStemmer();
				private BufferedReader br;
				private MultipleOutputs output; 
				
				static enum Counters{
					NUM_RECORDS, 
					DOC_LENGTH
				}
				
				protected void setup(Context context) throws IOException, InterruptedException {
								
				// load stopwords from given filename
				
					URI[] stopwordpath = context.getCacheFiles();
			        BufferedReader br = new BufferedReader(new FileReader(new File(stopwordpath[0].getPath()).getName()));
			        String stopWord; 
			        while ((stopWord = br.readLine()) != null) {
			        	stopWordList.add(stopWord);
			        } br.close(); 
				}
				
				@Override
				public void map(LongWritable key, Text value, Context context) throws
				IOException, InterruptedException {
								
					HashMap<String, Integer> termFrequencyMap = new HashMap<String, Integer>();

					int documentLength=0; 
								
					// split value input to filename and document contents
					String line = value.toString();
					String [] nameline = line.split("]]");
					 if (nameline.length == 1)
						System.out.println(line);
					String documentName = nameline[0].toLowerCase().replace("[", "").replace("]","");
					
					// remove punctuation from document text 
					String documentContents = nameline.length > 1 ? nameline[1].replaceAll("[^a-zA-Z ]", "").toLowerCase() : ""; 
					
					StringTokenizer tokenizer = new StringTokenizer(documentContents);
											
					while (tokenizer.hasMoreTokens()) {
						this.word.set(tokenizer.nextToken());
										
						// if word is not a stop word, add
						if (!stopWordList.contains(this.word.toString())){
							
							documentLength += 1; 
							
							// stem word with PorterStemmer
							String stemmedWord = stemmer.stem(this.word.toString());
							
							if (termFrequencyMap.containsKey(stemmedWord)) {
								
								Integer frequency = termFrequencyMap.get(stemmedWord); 
								
								Integer tf = frequency + 1; 
														
								termFrequencyMap.put(stemmedWord, tf);
								
							} else {
								termFrequencyMap.put(stemmedWord, 1); 
							}
						} 
					} 
					
					context.getCounter(Counters.NUM_RECORDS).increment(1);
					context.getCounter(Counters.DOC_LENGTH).increment(documentLength);
					
					IndexWritable docLengthWritable = new IndexWritable();
					
					docLengthWritable.setDocumentLength(new IntWritable(documentLength)); 
					
					context.write(new Text(documentName + ":"), docLengthWritable);
					
//					System.out.println(documentName.toString() + " " + docLengthWritable.getDocLength()); 

				
//					 writing the term frequencies to a map
					for (HashMap.Entry entry : termFrequencyMap.entrySet()) {
			            	            
			            Text term = new Text(entry.getKey().toString()); 
			            Integer termFreq = (Integer) entry.getValue(); 
			            
//			            System.out.println(term+ "-->"+ documentName + ": "+ termFreq);
			            
			            IndexWritable docFreqWritable = new IndexWritable(); 
			            docFreqWritable.setDocumentName(new Text(documentName));
			            docFreqWritable.setTermFrequency(new IntWritable(termFreq));
			            
//			            System.out.println(docFreqWritable.getDocName()+ " " 
//			            + docFreqWritable.getTermFrequency()+ " " + 
//			            		docFreqWritable.getDocLength());
			           
			            context.write(term, docFreqWritable); 
		      
			        }
					
				}
			}

			public static class Reduce extends Reducer<Text, IndexWritable, Text, Text> {
				
				private MultipleOutputs output; 
				
				public void setup(Context context) {
					output = new MultipleOutputs<>(context);
				}
				
				public void reduce(Text key, Iterable <IndexWritable> values, Context
						context) throws IOException, InterruptedException {
					
					if (key.toString().endsWith(":")) {
						
						// dealing with document frequencies 
										
						for (IndexWritable value : values) {
								
							Text docName = key; 
								
							String docLength = String.valueOf(value.getDocLength()); 
													
							output.write("DocumentFrequencies", docName, new Text(docLength));
						} 
						

						
					} else {
						
						// dealing with termFrequency values
						System.out.println();
						Text term = key; 
						
						ArrayList<String> tempVal = new ArrayList<String>(); 
																	
			
						for (final IndexWritable value : values) {
												
							String docName = value.getDocName().toString(); 
							Integer termFreq = value.getTermFrequency().get();
							String tempString = termFreq + "-" + docName;
							tempVal.add(tempString);
										
						}
						
						Collections.sort(tempVal, Collections.reverseOrder());
										
		                StringBuilder stringBuilder = new StringBuilder(100);
		                			
						for (String tempS : tempVal) {
							
							String[] vals = tempS.split("-");
							String tfFormatted = vals[1]+":"+vals[0]+", ";
		                    stringBuilder.append(tfFormatted);
						}

		                Text outputString = new Text(stringBuilder.toString());
		                System.out.println(term + "\t" + outputString); 
						output.write("TermFrequencies", term, outputString);
					
					}

				}
				  @Override
				  public void cleanup(Context context) throws IOException, InterruptedException
				  {
				      output.close();
				  }
			}

			
			
			@Override
			public int run(String[] args) throws Exception {
				
				Configuration conf = getConf();

				// TODO - REMOVE TWO LINES 
//				conf.set("mapreduce.framework.name", "local");
//		        conf.set("fs.defaultFS", "file:///");
		        
		        conf.set("textinputformat.record.delimiter", "\n[[");

				Job job = Job.getInstance(conf, "MyIndexer2");
				
				MultipleOutputs.addNamedOutput(job, "DocumentFrequencies", TextOutputFormat.class, Text.class, Text.class);
				MultipleOutputs.addNamedOutput(job, "TermFrequencies", TextOutputFormat.class, Text.class, Text.class);
				
				job.setJarByClass(MyIndexer.class);

				job.setMapperClass(Map.class);
				job.setReducerClass(Reduce.class);
				
				// TODO - COMMENT OUT STOPWORDPATH
			    // adds the path to stopwords.txt to a distributed cache (first argument)
//				String stopWordsPath = "src/main/resources/stopword-list-2.txt"; 
				 String stopWordsPath = "/user/2128536l/stopword-list.txt"; 

				job.addCacheFile(new Path(stopWordsPath).toUri());

				// sets file input to second argument
				job.setInputFormatClass(TextInputFormat.class);
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IndexWritable.class);
				
				// stops creation of empty file for context
				LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
//				job.setOutputFormatClass(TextOutputFormat.class);
				
//				job.setSortComparatorClass(ComparatorIndexWritable.class);

				
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				boolean success = job.waitForCompletion(true);

				Counters counters = job.getCounters();
				long numRecords = counters.getGroup("MyIndexer$Map$Counters").findCounter("NUM_RECORDS").getValue();
				long docLength = counters.getGroup("MyIndexer$Map$Counters").findCounter("DOC_LENGTH").getValue();
				int avgDocLength =  (int) Math.round((double) docLength / numRecords);
				
				System.out.println("==== OTHER METRICS ==="); 
				System.out.println("NUMBER OF DOCUMENTS: "+ numRecords); 
				System.out.println("AVERAGE DOCUMENT LENGTH: "+ avgDocLength + "\n"); 

				return (success ? 0 : 1);
			}

			public static void main(String[] args) throws Exception {
						
				System.exit(ToolRunner.run(new Configuration(), new MyIndexer2(), args));

				
			}
		}al static IntWritable one = new IntWritable(1);		public static final String Entry = null;
		private Text word = new Text();
		private Set<String> stopWordList = new HashSet<String>();
		PorterStemmer stemmer = new PorterStemmer();
		private BufferedReader br;
		private MultipleOutputs output; 
		
		static enum Counters{
			NUM_RECORDS, 
			DOC_LENGTH
		}
		
		
		protected void setup(Context context) throws IOException, InterruptedException {
						
		// load stopwords from given filename
		
			URI[] stopwordpath = context.getCacheFiles();
	        BufferedReader br = new BufferedReader(new FileReader(new File(stopwordpath[0].getPath()).getName()));
	        String stopWord; 
	        while ((stopWord = br.readLine()) != null) {
	        	stopWordList.add(stopWord);
	        } br.close(); 
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws
		IOException, InterruptedException {
						
			HashMap<String, String[]> termFrequencyMap = new HashMap<String, String[]>();
			int documentLength=0; 
						
			// split value input to filename and document contents
			String line = value.toString();
			String [] nameline = line.split("]]");
			 if (nameline.length == 1)
				System.out.println(line);
			String documentName = nameline[0].toLowerCase().replace("[", "").replace("]","");
			
			// remove punctuation from document text 
			String documentContents = nameline.length > 1 ? nameline[1].replaceAll("[^a-zA-Z ]", "").toLowerCase() : ""; 
			
			StringTokenizer tokenizer = new StringTokenizer(documentContents);
									
			while (tokenizer.hasMoreTokens()) {
				this.word.set(tokenizer.nextToken());
								
				// if word is not a stop word, add
				if (!stopWordList.contains(this.word.toString())){
					
					documentLength += 1; 
					
					// stem word with PorterStemmer
					String stemmedWord = stemmer.stem(this.word.toString());
					
					if (termFrequencyMap.containsKey(stemmedWord)) {
						
						String [] vals = termFrequencyMap.get(stemmedWord); 
						String docName = vals[0]; 
						Integer tf = Integer.parseInt(vals[1]) + 1; 
						String [] inputVals = new String[] {docName, String.valueOf(tf)};
						
						termFrequencyMap.put(stemmedWord, inputVals);
					} else {
						String [] inputVals = new String [] {documentName, String.valueOf(1)}; 
						termFrequencyMap.put(stemmedWord, inputVals); 
					}
				} 
			} 
			Text docL = new Text (String.valueOf(documentLength)); 
			context.getCounter(Counters.NUM_RECORDS).increment(1);
			context.getCounter(Counters.DOC_LENGTH).increment(documentLength);
            // adding : to document name to distinguish between outputs

			context.write(new Text(documentName+":"), docL);
			

			// writing the term frequencies to a map
			for (HashMap.Entry entry : termFrequencyMap.entrySet()) {
//	            System.out.print(entry.getKey()+ "-->"); 
	            String [] vals = (String[]) entry.getValue(); 
	            String output = vals[0] + "-=-=-" + vals[1];	      
	            Text k = new Text(entry.getKey().toString()); 

	            context.write(k, new Text(output)); 
	            
	        }
			
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private MultipleOutputs output; 
		
		public void setup(Context context) {
			output = new MultipleOutputs<>(context);
		}
		
		public void reduce(Text key, Iterable <Text> values, Context
				context) throws IOException, InterruptedException {
		
		
			// dealing with document frequency
			if (key.toString().endsWith(":")){	
				
				for (Text v : values) {
					String docName = key.toString().replace("*", "");
					
//					System.out.println(key.toString()+": "+v.toString());
					output.write("DocumentFrequencies", key, new Text(v.toString()));
				}
					
						
				}else {
					
					// otherwise we are dealing with the term frequency
										
					HashMap<String, Integer> unsortedFreq = new HashMap<String, Integer>();
                    StringBuilder stringBuilder = new StringBuilder(100);
//
					for (Text v : values) {
						
						String[] tfList = v.toString().split("-=-=-"); 
                        String docName = tfList[0];
                        String stringFreq = tfList[1].replaceAll("[^0-9]", "");
                        try { 
	                        Integer freq = Integer.parseInt(stringFreq); 
	                        unsortedFreq.put(docName, freq);
	//						System.out.println(docName + "-" + String.valueOf(freq));
	                        
                        } catch (Exception e) {
                        	
                        	System.out.println("exception=" + stringFreq + "==" +  v.toString() );
                        	
                        }
					}
					
//                    HashMap<String, Integer> sortedFreq = unsortedFreq.entrySet().stream()
//                            .sorted(HashMap.Entry.comparingByValue(Comparator.reverseOrder()))
//                            .collect(Collectors.toMap(HashMap.Entry::getKey, HashMap.Entry::getValue,
//                                    (oldValue, newValue) -> oldValue, LinkedHashMap::new));
                    
                    for (HashMap.Entry entry : unsortedFreq.entrySet()) {
                        String tfFormatted = entry.getKey().toString()+ ":"+ entry.getValue().toString() + ", "; 
                        stringBuilder.append(tfFormatted);
                    }
                    
//                    System.out.println(key.toString() + "-> " + stringBuilder.toString());
                    Text outputString = new Text(stringBuilder.toString());
					output.write("TermFrequencies", key, outputString);
				}
		}
		  @Override
		  public void cleanup(Context context) throws IOException, InterruptedException
		  {
		      output.close();
		  }
	}

	
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();

		// TODO - REMOVE TWO LINES 
		conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        
        conf.set("textinputformat.record.delimiter", "\n[[");

		Job job = Job.getInstance(conf, "WordCount-v1");
		
		MultipleOutputs.addNamedOutput(job, "DocumentFrequencies", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "TermFrequencies", TextOutputFormat.class, Text.class, Text.class);
		
		job.setJarByClass(MyIndexer.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		// TODO - COMMENT OUT STOPWORDPATH
	// adds the path to stopwords.txt to a distributed cache (first argument)
		String stopWordsPath = "src/main/resources/stopword-list-2.txt"; 
		// String stopWordsPath = "/user/2128536l/stopword-list.txt"; 

		job.addCacheFile(new Path(stopWordsPath).toUri());

		// sets file input to second argument
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// stops creation of empty file for context
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

		Counters counters = job.getCounters();
		long numRecords = counters.getGroup("MyIndexer$Map$Counters").findCounter("NUM_RECORDS").getValue();
		long docLength = counters.getGroup("MyIndexer$Map$Counters").findCounter("DOC_LENGTH").getValue();
		int avgDocLength =  (int) Math.round((double) docLength / numRecords);
		
		System.out.println("==== OTHER METRICS ==="); 
		System.out.println("NUMBER OF DOCUMENTS: "+ numRecords); 
		System.out.println("AVERAGE DOCUMENT LENGTH: "+ avgDocLength + "\n"); 

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
				
		System.exit(ToolRunner.run(new Configuration(), new MyIndexer(), args));

		
	}
}