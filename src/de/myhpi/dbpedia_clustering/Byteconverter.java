package de.myhpi.dbpedia_clustering;
class Byteconverter {

	static int bitAt(byte[] bytes, int index) {
		return (bytes[index / 8] >>> (index % 8)) & 1;
	}

	static byte byteAt(byte[] bytes, int index) {
		return Byteconverter.bitToByte(Byteconverter.bitAt(bytes, index));
	}

	static int fromSigned(byte val) {
		if (val < 0)
			return 256 + val;
		return val;
	}

	static byte toSigned(int val) {
		if (val > 127)
			return (byte) (val - 256);
		return (byte) val;
	}

	static byte bitToByte(int val) {
		return Byteconverter.toSigned(255 * val);
	}

	static byte ratioToByte(int num, int den) {
		return Byteconverter.toSigned((255 * num) / den);
	}

}