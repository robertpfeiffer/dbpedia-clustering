package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.io.BytesWritable;

class EuclideanDistance implements Distance<BytesWritable, BytesWritable> {
	
	public double between(BytesWritable center, BytesWritable subject) {
		long distance = 0;
		byte[] centerBytes = center.getBytes();
		byte[] subjectBytes = subject.getBytes();
		
		// assert(center.getLength()/8 == subject.getLength());
			
		for (int i = 0; i < center.getLength(); i++) {
		    distance += Math.abs(Math.pow(Byteconverter.fromSigned(centerBytes[i]),2) 
					 - Math.pow(Byteconverter.fromSigned(Byteconverter.byteAt(subjectBytes, i)),2));
		}
		return distance;
	}
}