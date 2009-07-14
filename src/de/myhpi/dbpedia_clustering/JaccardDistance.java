package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.io.BytesWritable;

public class JaccardDistance implements Distance<BytesWritable, BytesWritable> {
	
	public double between(BytesWritable center, BytesWritable subject) {
	    byte[] subjectBits = subject.getBytes();
	    byte[] subjectBytes = new byte[center.getLength()];
	    for (int i = 0; i < center.getLength(); i++)
		subjectBytes[i]=Byteconverter.byteAt(subjectBytes, i);
	    subject = new  BytesWritable(subjectBytes);
	    return 1.0 -
		FuzzyByteSet.cardinality(FuzzyByteSet.intersection(subject,center))/
		FuzzyByteSet.cardinality(FuzzyByteSet.union(subject,center));
	}
}