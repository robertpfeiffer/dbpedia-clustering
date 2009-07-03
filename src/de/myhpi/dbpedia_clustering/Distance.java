package de.myhpi.dbpedia_clustering;

interface Distance<T1, T2> {
	public long between(T1 center, T2 subject);
}