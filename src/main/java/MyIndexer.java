
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

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.PorterStemmer;

public class MyIndexer extends Configured implements Tool {
	
	
	static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		private final static IntWritable one = new IntWritable(1);
		public static final String Entry = null;
		private Text word = new Text();
		private Set<String> stopWordList = new HashSet<String>();
		PorterStemmer stemmer = new PorterStemmer();
		private BufferedReader br;
		private MultipleOutputs output; 
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
			String [] nameline = line.split("]]\n");
			String documentName = nameline[0].toLowerCase().replace("[", "").replace("]","");
			
			// remove punctuation from document text 
			String documentContents = nameline[1].replaceAll("[^a-zA-Z ]", "").toLowerCase(); 
			
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
            // adding * to document name to distinguish between outputs

			context.write(new Text(documentName+":"), docL);

			// writing the term frequencies to a map
			for (HashMap.Entry entry : termFrequencyMap.entrySet()) {
//	            System.out.print(entry.getKey()+ "-->"); 
	            String [] vals = (String[]) entry.getValue(); 
	            String output = vals[0] + "--" + vals[1];	      
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
		
		
			// ===== DEALING WITH DOCUMENT FREQUENCY =====
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
						
						String[] tfList = v.toString().split("--"); 
                        String docName = tfList[0];
                        String stringFreq = tfList[1].replaceAll("[^0-9]", "");
                        try { 
	                        Integer freq = Integer.parseInt(stringFreq); 
	                        unsortedFreq.put(docName, freq);
	//						System.out.println(docName + "-" + String.valueOf(freq));
                        } catch (Exception e) {
                        	
                        	System.out.println("=======EXCEPTION====" + stringFreq + "==" +  v.toString() );
                        	
                        }
//                      
					}
                    HashMap<String, Integer> sortedFreq = unsortedFreq.entrySet().stream()
                            .sorted(HashMap.Entry.comparingByValue(Comparator.reverseOrder()))
                            .collect(Collectors.toMap(HashMap.Entry::getKey, HashMap.Entry::getValue,
                                    (oldValue, newValue) -> oldValue, LinkedHashMap::new));
                    

                    for (HashMap.Entry entry : sortedFreq.entrySet()) {
                        String tfFormatted = entry.getKey().toString()+ ":"+ entry.getValue().toString() + ", "; 
                        stringBuilder.append(tfFormatted);
//                      System.out.print("\t"+entry.getKey()+ ":"+ entry.getValue());
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

		// The following two lines instruct Hadoop/MapReduce to run in local
		// mode. In this mode, mappers/reducers are spawned as thread on the
		// local machine, and all URLs are mapped to files in the local disk.
		// Remove these lines when executing your code against the cluster.
		conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        
        conf.set("textinputformat.record.delimiter", "\n[[");

		Job job = Job.getInstance(conf, "WordCount-v1");
		
		MultipleOutputs.addNamedOutput(job, "DocumentFrequencies", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "TermFrequencies", TextOutputFormat.class, Text.class, Text.class);
		
		job.setJarByClass(MyIndexer.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
	// adds the path to stopwords.txt to a distributed cache (first argument)
		job.addCacheFile(new Path(args[0]).toUri());

		// sets file input to second argument
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// stops creation of empty file for context
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
				
		System.exit(ToolRunner.run(new Configuration(), new MyIndexer(), args));

		
	}
}