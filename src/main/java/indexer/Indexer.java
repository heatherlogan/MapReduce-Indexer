package indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.net.URI;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.PorterStemmer;

public class Indexer extends Configured implements Tool {
	
	
	static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Set<String> stopWordList = new HashSet<String>();
		PorterStemmer stemmer = new PorterStemmer();
		private BufferedReader fis;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			
			// load stopwords from given filename
			URI[] stopwordpath = context.getCacheFiles();
	        BufferedReader fis = new BufferedReader(new FileReader(new File(stopwordpath[0].getPath()).getName()));
	        String stopWord; 
	        while ((stopWord = fis.readLine()) != null) {
	        	stopWordList.add(stopWord);
	        }

		}
		
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws
		IOException, InterruptedException {
						
			// split value input to filename and document contents
			String line = value.toString();
			String [] nameline = line.split("]]\n");
			String fileName = nameline[0].toLowerCase().replace("[", "").replace("]","");
			
			// remove punctuation from document text 
			String documentContents = nameline[1].replaceAll("[^a-zA-Z ]", "").toLowerCase(); 
			
			StringTokenizer tokenizer = new StringTokenizer(documentContents);
									
			while (tokenizer.hasMoreTokens()) {
				
				this.word.set(tokenizer.nextToken());
				
				// if word is not a stop word, add
				if (!stopWordList.contains(this.word.toString())){
					String stemmedWord = stemmer.stem(this.word.toString());
					Text stemmedText = new Text(stemmedWord); 
					context.write(stemmedText, one);
				}
				
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context
				context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable value: values)
				sum += value.get();
			context.write(key, new IntWritable(sum));
//			System.out.println(key.toString());	
//			System.out.println(new IntWritable(sum));		

			
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
		job.setJarByClass(Indexer.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		// adds the path to stopwords.txt to a distributed cache (first argument)
		job.addCacheFile(new Path(args[0]).toUri());

		
		// sets file input to second argument
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
				
		System.exit(ToolRunner.run(new Configuration(), new Indexer(), args));

		
	}
}