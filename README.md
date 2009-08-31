Clustering of DBPedia Subjects
==============================
 
Seminar Map/Reduce Algorithms on Hadoop 
---------------------------------------


##Schritt 1: Kompilieren des Projektes
jar mit ant erstellen

    ant make-jar

##Schritt 2: Erstellen der Sequencedatei
Eine Datei mit den Namen muss aus der ersten pivotdatei generiert werden.

    tail -n 1 infobox_pivot_part1 > names

Die Klasse BitsToSeqFile muss mit der pivot-Binärdatei, der Namensdatei und dem Namen 
der gewünschten Ausgabedatei für die Subjekte aufgerufen werden

    java -jar dist/clustering.jar de.myhpi.BitsToSeqFile infobox_pivot_part2 names subjects.seq

##Schritt 3: Erstellen der Clusterzentren
Die Klasse GenerateClusters muss mit der Subjektdatei, der Namensdatei und dem Namen 
der gewünschten Ausgabedatei für die Subjekte aufgerufen werden. Weitere benötigte 
Argumente sind die Anzahl der Attribute und die Anzahl der zu erzeugenden Cluster.

    java -jar dist/clustering.jar de.myhpi.GenerateClusters subjects.seq centers.seq 42644 100

##Schritt 4: Kopieren der Eingabedateien ins HDFS
Danach müssen die Subjektdatei, die Clusterzetrendatei und die Datei config.xml in das
HDFS kopiert werden. Gegenbenenfalls kann die config.xml angepasst werden.

##Schritt 5: Jobs ausführen
hadoop jar mit dem Programmnamen "k-means" und der Subjektdatei, der Zentrendatei und
dem Ausgabepfad aufrufen

   hadoop jar dist/clustering.jar k-means subjects.seq centers.seq output-dir

##Schritt 6: Ausgabedaten aus dem HDFS kopieren
Nachdem das Programm die Jobs ausgeführt hat, können die Ausgabedaten auf das lokale 
Dateisystem kopiert und von Menschen gelesen werden
