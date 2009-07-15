package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.io.BytesWritable;


class FuzzyByteSet {
    public static BytesWritable union(BytesWritable aB ,BytesWritable bB) {
	int length=aB.getLength();
	byte[] a = aB.getBytes(),
	    b = bB.getBytes(),
	    result = new byte[length];
	for (int i=0; i<length;i++)
	    result[i]=Byteconverter.fromSigned(a[i])>Byteconverter.fromSigned(b[i])
		?a[i]:b[i];
	return new BytesWritable(result);
    }

    public static BytesWritable intersection(BytesWritable aB ,BytesWritable bB) {
	int length=aB.getLength();
	byte[] a = aB.getBytes(),
	    b = bB.getBytes(),
	    result = new byte[length];
	for (int i=0; i<length;i++)
	    result[i]=Byteconverter.fromSigned(a[i])<Byteconverter.fromSigned(b[i])
		?a[i]:b[i];
	return new BytesWritable(result);
    }

    public static BytesWritable complement(BytesWritable aB) {
	int length=aB.getLength();
	byte[] a = aB.getBytes(),
	    result = new byte[length];
	for (int i=0; i<length;i++)
	    result[i]=Byteconverter.toSigned(255-Byteconverter.fromSigned(a[i]));
	return new BytesWritable(result);
    }

     public static double cardinality(BytesWritable aB) {
	 int length=aB.getLength();
	 byte[] a = aB.getBytes();
	 double result = 0;
	 for (int i=0; i<length;i++)
	    result+=Byteconverter.fromSigned(a[i]);
	 return result;
    }
}