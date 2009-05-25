package de.myhpi.dbpedia_clustering;

import de.myhpi.dbpedia_clustering.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class ClusterDriver 
{  
	public static void main(String argv[])
	{
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try
		{
			pgd.addClass("wordcount", WordCount.class,
				     "A map/reduce program that counts the words in the input files.");
			pgd.addClass("k-means", WordCount.class,
				     "Clustering of DBPedia Subjects");
			pgd.driver(argv);
			exitCode = 0;
		} catch (Throwable t) //wtf?
		{	
			;//HACK: ignore
		} finally
		{
			System.exit(exitCode);
		}
	}
}
	
