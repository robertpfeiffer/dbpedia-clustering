package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.io.BytesWritable;

public class JaccardDistance implements Distance<BytesWritable, BytesWritable> {

	public double between(BytesWritable center, BytesWritable subject) {
		byte[] subjectBits = subject.getBytes();
		byte[] centerBytes = center.getBytes();
		long i_card, u_card;
		i_card = u_card = 0;
		int a, b, length;

		length = center.getLength();
		for (int i = 0; i < length; i++) {
			a = Byteconverter.uByteAt(subjectBits, i);
			b = Byteconverter.fromSigned(centerBytes[i]);
			if (a > b) {
				u_card += a;
				i_card += b;
			} else {
				u_card += b;
				i_card += a;
			}
		}

		if (u_card == 0 || u_card == i_card)
			return 0.0;
		return 1.0 - ((double) i_card) / u_card;
	}

	public double index(BytesWritable center1, BytesWritable center2) {
		byte[] center1Bytes = center1.getBytes();
		byte[] center2Bytes = center2.getBytes();
		long i_card, u_card;
		i_card = u_card = 0;
		int a, b, length;

		length = center1.getLength();
		for (int i = 0; i < length; i++) {
			a = Byteconverter.fromSigned(center1Bytes[i]);
			b = Byteconverter.fromSigned(center2Bytes[i]);
			if (a > b) {
				u_card += a;
				i_card += b;
			} else {
				u_card += b;
				i_card += a;
			}
		}

		if (u_card == 0 || u_card == i_card)
			return 0.0;
		return ((double) i_card) / u_card;
	}
}