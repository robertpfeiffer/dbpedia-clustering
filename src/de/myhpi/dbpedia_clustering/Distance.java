package de.myhpi.dbpedia_clustering;

interface Distance<T1, T2> {
	public double between(T1 center, T2 subject);
}