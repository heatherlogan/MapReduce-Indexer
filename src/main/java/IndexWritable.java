import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IndexWritable implements WritableComparable<IndexWritable> {
	

	private IntWritable docLength; 

	private Text docName; 
	private IntWritable termFrequency;
	
	
	// constructors
	
	public IndexWritable() {
		docLength = new IntWritable(); 
		docName = new Text(); 
		termFrequency = new IntWritable();
	
	}
	
	public IndexWritable(Text docName, IntWritable termFrequency) {
		super();
		setDocumentName(docName);
		setTermFrequency(termFrequency);
	}
	
	public IndexWritable(IntWritable docLength) {
		setDocumentLength(docLength); 
	}
	
	
	// setters
	
	public void setDocumentLength(IntWritable docLength) {
		this.docLength = docLength; 
	}
	
	public void setDocumentName(Text docName) {
		this.docName = docName; 
	}
	
	public void setTermFrequency(IntWritable termFrequency) {
		this.termFrequency = termFrequency; 
	}
	
	
	// getters
	
	public IntWritable getDocLength() {
		return docLength; 
	}
	
	public Text getDocName()	{
		return this.docName;
	}
	
	public IntWritable getTermFrequency() {
		return this.termFrequency; 
	}

	
	
	// methods
	
	@Override
	public void readFields(DataInput in) throws IOException {
		docLength.readFields(in);
		docName.readFields(in);
		termFrequency.readFields(in);
		
	
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		docLength.write(out);
		docName.write(out);
		termFrequency.write(out); 
		
	}

	@Override
	public String toString() {
		return "IndexWritable [docLength=" + docLength + ", docName=" + docName + ", termFrequency=" + termFrequency
				+ "]";
	}
	
	@Override
	public int compareTo(IndexWritable idx) {
				
		int thisTF = this.termFrequency.get();
		int otherTF = idx.getTermFrequency().get(); 
		
        return (thisTF < otherTF ? -1 : (thisTF==otherTF ? 0 : 1));
		
	} 

	
	@Override
	public int hashCode() {
		
		final int prime = 31;
		int result = 1;
		result = prime * result + ((docLength == null) ? 0 : docLength.hashCode());
		result = prime * result + ((docName == null) ? 0 : docName.hashCode());
		result = prime * result + ((termFrequency == null) ? 0 : termFrequency.hashCode());
		return result;
		
	}
	

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexWritable other = (IndexWritable) obj;
		if (docLength == null) {
			if (other.docLength != null)
				return false;
		} else if (!docLength.equals(other.docLength))
			return false;
		if (docName == null) {
			if (other.docName != null)
				return false;
		} else if (!docName.equals(other.docName))
			return false;
		if (termFrequency == null) {
			if (other.termFrequency != null)
				return false;
		} else if (!termFrequency.equals(other.termFrequency))
			return false;
		return true;
	}




	

	


}
