package de.myhpi.dbpedia_clustering;

import java.util.HashSet;
import org.apache.hadoop.io.BytesWritable;

public class JaccardDisctance implements Distance<BytesWritable, BytesWritable> {
	
	public long between(BytesWritable center, BytesWritable subject) {
		long distance = 0;
		byte[] centerBytes = center.getBytes();
		byte[] subjectBytes = subject.getBytes();
		HashSet<Integer> centerValues = new HashSet<Integer>();
		HashSet<Integer> subjectValues = new HashSet<Integer>();
			
		for (int i = 0; i < center.getLength(); i++) {
			centerValues.add(Byteconverter.fromSigned(centerBytes[i]));
			subjectValues.add(Byteconverter.fromSigned(Byteconverter.byteAt(subjectBytes, i)));
			//distance += Math.abs(centerBytes[i] - Byteconverter.byteAt(subjectBytes, i));
		}
		System.out.println("-----");
		System.out.println(centerValues);
		System.out.println(subjectValues);
		return distance;
	}
}