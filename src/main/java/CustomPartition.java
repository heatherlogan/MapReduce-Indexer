import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartition extends Partitioner<IndexWritable, Text> {
	
	
	@Override
	public int getPartition(IndexWritable key, Text value, int numPartitions) {
			
		// if it's a doclength writable, partition by docName
		if (value.toString().equals("DocLengths")) {
			
			Text realKey = key.getDocName(); 
			
			return Math.abs((realKey.toString().hashCode()) % numPartitions);

		} else {
			
			// else we want to partition by term
			
			Text realKey = key.getTerm(); 
			
			return Math.abs((realKey.toString().hashCode()) % numPartitions);
			
		}
	}
}
