package de.myhpi.dbpedia_clustering;

interface Distance<T1, T2> {
	public double between(T1 center, T2 subject);
	public double index(T1 center1, T2 center2);
}