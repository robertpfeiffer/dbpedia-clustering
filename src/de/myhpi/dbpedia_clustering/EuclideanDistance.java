package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.io.BytesWritable;

class EuclideanDistance implements Distance<BytesWritable, BytesWritable> {

	public double between(BytesWritable center, BytesWritable subject) {
		long distance = 0;
		byte[] centerBytes = center.getBytes();
		byte[] subjectBytes = subject.getBytes();

		for (int i = 0; i < center.getLength(); i++) {
			distance += Math.pow(Byteconverter.fromSigned(centerBytes[i])
			    - Byteconverter.uByteAt(subjectBytes,i),2);
		}
		return Math.sqrt(distance);
	}
	
	public double index(BytesWritable center1, BytesWritable center2) {
		double distance = (double) 0.0;
		double maxDistance = (double) 0.0;
		byte[] center1Bytes = center1.getBytes();
		byte[] center2Bytes = center2.getBytes();

		// calculate Distance
		for (int i = 0; i < center1.getLength(); i++) {
			distance += Math.pow(Byteconverter.fromSigned(center1Bytes[i])
			    - Byteconverter.fromSigned(center2Bytes[i]),2);
		}
		
		// calculate max Distance: (255-0)**2 = 65025
		maxDistance = (double) 65025 * center1.getLength();
		
		return distance / maxDistance;
	}
}