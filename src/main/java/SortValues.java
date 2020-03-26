import java.util.Comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortValues implements Comparator<IndexWritable> {
	
	public int compare(IndexWritable obj1, IndexWritable obj2) {
		
		int result = obj1.getDocName().compareTo(obj2.getDocName()); 
		
		if (result == 0) {
			
			return -obj1.getTermFrequency().compareTo(obj2.getTermFrequency());
			

			
		}
		
		return result; 
	}
	

}
