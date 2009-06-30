package de.myhpi.dbpedia_clustering;

class ByteBitDistance implements Distance<byte[], byte[]> {
	
	public long between(byte[] center, byte[] subject) {
		long distance = 0;
		for (int i = 0;i<center.length;i++) {
			distance += Math.abs(Byteconverter.fromSigned(center[i]) - Byteconverter.fromSigned(Byteconverter.bitToByte(Byteconverter.bitAt(subject, i))));
		}
		return distance;
	}
}