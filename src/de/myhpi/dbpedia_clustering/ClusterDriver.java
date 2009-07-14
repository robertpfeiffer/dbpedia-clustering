package de.myhpi.dbpedia_clustering;
import org.apache.hadoop.util.ProgramDriver;

public class ClusterDriver
{  
	public static void main(String argv[])
	{
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try
		{
			pgd.addClass("k-means", KMeans.class, "Clustering of DBPedia Subjects");
			pgd.driver(argv);
			exitCode = 0;
		} catch (Throwable t) //wtf?
		{
		    t.printStackTrace();
		} finally
		{
			System.exit(exitCode);
		}
	}
}
	
