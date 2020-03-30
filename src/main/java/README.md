Big Data 2020 Assessed Exercise - MapReduce Indexer

MyIndexer.java contains the code for mapper, reducer and driver.

IndexWritable.java contains the code for the custom writable object output by the mapper. 

ComparatorIndexWritable contains the comparator for secondary sorting. 

CustomPartition contains the code to partition records by document name, or term. 