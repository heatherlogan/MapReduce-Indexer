import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ComparatorIndexWritable extends WritableComparator {
	
	
	public ComparatorIndexWritable() {
		
		super(IndexWritable.class, true); 
		
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		IndexWritable obj1 = (IndexWritable)a;
		IndexWritable obj2 = (IndexWritable)b; 
		
		
//		return -obj1.getTermFreqMap().compareTo(obj2.getTermFreqMap()); 
		return 0; 

		
	}
	

}
