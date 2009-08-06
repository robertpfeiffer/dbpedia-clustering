package de.myhpi.dbpedia_clustering;

interface Distance<T1, T2> {
	public double between_center_subject(T1 center, T2 subject);
	public double between_center_center(T1 center1, T2 center2);
}