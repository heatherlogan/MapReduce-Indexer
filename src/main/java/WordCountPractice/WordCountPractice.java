package WordCountPractice;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCountPractice {
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException,InterruptedException {
			
			// key = byte offset
			// value = string value
			
			String line = value.toString();
			
			StringTokenizer tokenizer = new StringTokenizer(line); 
			
			while(tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				context.write(value, new IntWritable(1));
			}
			
		}

	}
	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException,InterruptedException {
			
			int sum = 0;
			
			for (IntWritable x: values) {
				sum += x.get(); 
			}
			
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main (String [] args) throws Exception {
		
		
		
		
		Configuration conf = new Configuration();
		
		// The following two lines instruct Hadoop/MapReduce to run in local
		// mode. In this mode, mappers/reducers are spawned as thread on the
		// local machine, and all URLs are mapped to files in the local disk.
		// Remove these lines when executing your code against the cluster.
		conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
		
		Job job = Job.getInstance(conf, "WordCountPractice");
		
		job.setJarByClass(WordCountPractice.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class); 
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		
		
	}
	
	

}
