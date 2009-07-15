package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.io.BytesWritable;

public class JaccardDistance implements Distance<BytesWritable, BytesWritable> {	
	public double between(BytesWritable center, BytesWritable subject) {
	    byte[] subjectBits = subject.getBytes();
	    byte[] centerBytes = center.getBytes();
	    long i_card,u_card;
	    i_card=u_card=0;
	    int a,b,length;
	    length=center.getLength();
	    for (int i=0; i<length;i++) {
		a=Byteconverter.uByteAt(subjectBits, i);
		b=Byteconverter.fromSigned(centerBytes[i]);
		if(a>b) {
		    u_card+=a;
		    i_card+=b;
		} else {
		    u_card+=b;
		    i_card+=a;
		}
	    }
	    if(u_card==0)
		return 0.0;
	    return 1.0 - (1.0*i_card)/u_card;
	}
}