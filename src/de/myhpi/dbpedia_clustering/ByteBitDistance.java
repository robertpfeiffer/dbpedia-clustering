package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.io.BytesWritable;

class ByteBitDistance implements Distance<BytesWritable, BytesWritable> {
	
	public long between(BytesWritable center, BytesWritable subject) {
		long distance = 0;
		byte[] centerBytes = center.getBytes();
		byte[] subjectBytes = subject.getBytes();
		
		// assert(center.getLength()/8 == subject.getLength());
			
		for (int i = 0; i < center.getLength(); i++) {
			distance += Math.abs(Byteconverter.fromSigned(centerBytes[i]) - Byteconverter.fromSigned(Byteconverter.byteAt(subjectBytes, i)));
		}
		return distance;
	}
}