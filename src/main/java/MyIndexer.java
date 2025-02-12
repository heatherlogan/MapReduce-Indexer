
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.PorterStemmer;

public class MyIndexer extends Configured implements Tool {
	
	
	static class Map extends Mapper<LongWritable, Text, IndexWritable, Text> {
		
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
						
		// load stopwords from url into set of stopwords
		
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
						
			// term frequency map stores each term; occurrence pair per document. 
			HashMap<String, Integer> termFrequencyMap = new HashMap<String, Integer>();

			int documentLength=0; 
						
			// Split value input to filename and document contents
			String line = value.toString();
			String [] nameline = line.split("]]");
			
			// checks for invalid docs
			 if (nameline.length == 1)
				System.out.println(line);
			
			 // formats document name 
			 String documentName = nameline[0].toLowerCase().replace("[", "").replace("]","");
			
			// Remove punctuation from document text 
			String documentContents = nameline.length > 1 ? nameline[1].replaceAll("[^a-zA-Z ]", "").toLowerCase() : ""; 
			
			StringTokenizer tokenizer = new StringTokenizer(documentContents);
									
			while (tokenizer.hasMoreTokens()) {
				this.word.set(tokenizer.nextToken());
								
				// if word is not a stop word, add
				if (!stopWordList.contains(this.word.toString())){
					
					// increment doc counter 
					documentLength += 1; 
					
					// stem word with PorterStemmer
					String stemmedWord = stemmer.stem(this.word.toString());
					
					// if word has already been seen, get its occurrence and increment
					if (termFrequencyMap.containsKey(stemmedWord)) {
						
						Integer frequency = termFrequencyMap.get(stemmedWord); 
						Integer tf = frequency + 1; 
						termFrequencyMap.put(stemmedWord, tf);
						
					} else {
						
						// otherwise word is unseen and occurrence is now 1
						termFrequencyMap.put(stemmedWord, 1); 
					}
				} 
			} 
			
			// increment counters for avg doc length + num. records metrics 
			context.getCounter(Counters.NUM_RECORDS).increment(1);
			context.getCounter(Counters.DOC_LENGTH).increment(documentLength);
			
			IndexWritable docLengthWritable = new IndexWritable();
			docLengthWritable.setDocumentName(new Text(documentName));
			docLengthWritable.setDocumentLength(new IntWritable(documentLength)); 
			
			context.write(docLengthWritable, new Text("DocLengths"));
		
//			 writing the term frequencies to a map
			for (HashMap.Entry entry : termFrequencyMap.entrySet()) {
	            	            
	            Text term = new Text(entry.getKey().toString()); 
	            Integer termFreq = (Integer) entry.getValue(); 
	            
	            IndexWritable docFreqWritable = new IndexWritable(); 
	      
	            docFreqWritable.setTerm(term);
	            docFreqWritable.setDocumentName(new Text(documentName));
	            docFreqWritable.setTermFrequency(new IntWritable(termFreq));

	            context.write(docFreqWritable, new Text("TermFreqs")); 
      
	        }
		}
	}
	
	

	public static class Reduce extends Reducer<IndexWritable, Text, Text, Text> {
		
		private MultipleOutputs output; 
		private StringBuilder previousTerm ; 
		private StringBuilder outputFormatted; 
		
		public void setup(Context context) {
			
			output = new MultipleOutputs<>(context);
			previousTerm = new StringBuilder(); 
			outputFormatted = new StringBuilder(); 
			
		}
		
		public void reduce(IndexWritable compositeKey, Iterable <Text> values, Context
				context) throws IOException, InterruptedException {
			
			for (Text value: values) {
				
				if (value.toString().equals("DocLengths")) {
					
					// dealing with doc frequencies
					int docLength = compositeKey.getDocLength().get(); 
//					
					output.write("DocumentFrequencies", compositeKey.getDocName(), new Text(String.valueOf(docLength)));
				}
				else {
					
					// dealing with term frequencies
					
					Text term = compositeKey.getTerm(); 
					Text docName = compositeKey.getDocName(); 
					int termFreq = compositeKey.getTermFrequency().get(); 
					String formattedFreq = docName +":"+ String.valueOf(termFreq)+", "; 
					
					// if term=previous term, then add the new term frequency info to output
					
					if (previousTerm.toString().equals(term.toString())) {
						
						outputFormatted.append(formattedFreq); 
						
						
					} else {
						
						// onto a new term: write previous term+ string to output
																								
		   	   			output.write("TermFrequencies", previousTerm, outputFormatted);
						
		   	   			// reset values and append current 
	
						previousTerm = new StringBuilder(); 
						previousTerm.append(term.toString());
						
						outputFormatted = new StringBuilder(); 	
						outputFormatted.append("| "); 
						outputFormatted.append(formattedFreq); 
					}

				}
			}
		}
		
		  @Override
		  public void cleanup(Context context) throws IOException, InterruptedException
		  {
			// write last term 
 	   		output.write("TermFrequencies", previousTerm, outputFormatted);
		    output.close();
		  }
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
//		conf.set("mapreduce.framework.name", "local");
//      conf.set("fs.defaultFS", "file:///");

		// splits each document by the heading 
		conf.set("textinputformat.record.delimiter", "\n[[");
		
		// increasing memory assigned to reducers to fix memory issue
		conf.set("mapreduce.reduce.java.opts",  "-Xmx2048m");

		Job job = Job.getInstance(conf, "MyIndexer");
		
		// two outputs for document / term freq information 
		MultipleOutputs.addNamedOutput(job, "DocumentFrequencies", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "TermFrequencies", TextOutputFormat.class, Text.class, Text.class);
		
		job.setJarByClass(MyIndexer.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setNumReduceTasks(150);
		
	    // adds the path to stopwords.txt to a distributed cache (first argument)
		String stopWordsPath = "/user/2128536l/stopword-list.txt";
				
		job.addCacheFile(new Path(stopWordsPath).toUri());

		// sets file input to second argument
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setOutputKeyClass(IndexWritable.class);
		job.setOutputValueClass(Text.class);
		
		// stops creation of empty file for context
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		// comparator and partition for secondary sorting
		job.setSortComparatorClass(ComparatorIndexWritable.class);
		job.setPartitionerClass(CustomPartition.class);

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
				
		System.exit(ToolRunner.run(new Configuration(), new MyIndexer(), args));

		
	}
}