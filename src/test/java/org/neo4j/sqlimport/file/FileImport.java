package org.neo4j.sqlimport.file;
//package org.neo4j.sqlimport.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class FileImport
{

    private static final String TARGET_DIR = "target/neo4j";
    private BatchInserterImpl neo4j;
    private BatchInserterIndexProvider index;
    private File nodeFile;
    private File relFile;
    protected String[] fields;
    protected BufferedReader br;
    protected String strLine;
    private int stepSize;


    public static void main(String[] args)
    {
        FileImport importer = new FileImport();
        

        try
        {
            FileImportTest.deleteFileOrDirectory( new File( TARGET_DIR ) );
            importer.setUp();
            importer.createNodes(1,50000);
            importer.importRelationships("RELATIONSHIP_TYPE1","01.dat",0);
            importer.tearDown();
        }
        catch (Exception e)
        {

        }       
    }

    @Before
    public void setUp() throws Exception
    {
        neo4j = new BatchInserterImpl( TARGET_DIR );
        index = new LuceneBatchInserterIndexProvider( neo4j );
        stepSize = 10000;
    }

    public void createNodes(int start, int end)
    {
        Map<String, Object> properties = new HashMap<String, Object>();

        for(int x=start; x<=end; x++)
        {
            if((x % stepSize) == 0){System.out.print(x+",");};
            neo4j.createNode(x,properties);
        }
    }

    public void importRelationships(String relationshipType, String fileName, int startingLine) throws Exception
    {
        File relFile = new File(fileName);
        System.out.println("Importing from " + relFile);

        Map<String, Object> properties = new HashMap<String, Object>();
        int lineCount = 0;

        try
        {
            br = new BufferedReader( new InputStreamReader( new FileInputStream(relFile ) ) );
            int source, dest;

            while ( ( strLine = br.readLine() ) != null )
            {
                lineCount++;
                
                String[] values = strLine.split( "," );
                source = Integer.parseInt(values[1]);
                dest = Integer.parseInt(values[2]);

                if (Integer.parseInt(values[0]) >= startingLine)
                {
                    if (source != dest)
                        neo4j.createRelationship( source, dest, DynamicRelationshipType.withName(relationshipType), properties );
        
                    if ( ( lineCount % stepSize ) == 0 )
                        System.out.print( lineCount +"," );
                }
            }

            int importCount = lineCount - startingLine + 1;
            System.out.println("imported " + importCount + " records starting at line " +startingLine +".");
            br.close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception
    {
        index.shutdown();
        neo4j.shutdown();
    }
}
