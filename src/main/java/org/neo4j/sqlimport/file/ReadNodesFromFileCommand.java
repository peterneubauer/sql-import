package org.neo4j.sqlimport.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class ReadNodesFromFileCommand implements FileImportCommand
{

    private final File nodeFile;
    protected final ArrayList<String> indexFields = new ArrayList<String>();
    protected String[] fields;
    private boolean useFirstColumnAsIds;
    protected BufferedReader br;
    protected String strLine;
    private int stepSize;

    public ReadNodesFromFileCommand( File nodeFile,
            boolean useFirstColumnAsIds, String... indexFields )
    {
        this.nodeFile = nodeFile;
        for ( int i = 0; i < indexFields.length; i++ )
        {
            this.indexFields.add( indexFields[i] );
        }
        this.useFirstColumnAsIds = useFirstColumnAsIds;

    }

    public void execute( BatchInserterImpl neo4j,
            LuceneIndexBatchInserterImpl index, int stepSize )
    {
        System.out.println("Importing from " + nodeFile);
        this.stepSize = stepSize;
        try
        {
            openFile();
            processBodyRecords( neo4j, index );
            // Close the input stream
            br.close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    protected void processBodyRecords( BatchInserterImpl neo4j,
            LuceneIndexBatchInserterImpl index ) throws IOException
    {
        int lineCount = 0;
        while ( ( strLine = br.readLine() ) != null )
        {
            lineCount++;
            // Print the content on the console
            // System.out.println( strLine );

            Map<String, Object> properties = new HashMap<String, Object>();
            String[] values = strLine.split( "," );
            int i = 0;
            if ( useFirstColumnAsIds ) i++;
            for ( ; i < values.length; i++ )
            {
                if ( values[i] != null && values[i].length() > 0 )
                {
                    properties.put( fields[i], values[i] );
                }
            }
            processRecord( neo4j, index, properties, values );
            report( lineCount );
        }
        System.out.println("imported " + lineCount + " records.");
    }

    private void report( int lineCount )
    {
        if ( ( lineCount % stepSize ) == 0 )
        {
            System.out.print( "." );
        }

    }

    protected void processRecord( BatchInserterImpl neo4j,
            LuceneIndexBatchInserterImpl index, Map<String, Object> properties,
            String[] values )
    {
        long nodeId = 0;
        if ( useFirstColumnAsIds )
        {
            neo4j.createNode( Integer.parseInt( values[0] ), properties );
        }
        else
        {
            nodeId = neo4j.createNode( properties );
        }
        // index
        for ( String key : properties.keySet() )
        {
            if ( indexFields.contains( key ) )
            {
                index.index( nodeId, key, properties.get( key ) );
            }
        }
    }

    private void openFile() throws FileNotFoundException, IOException
    {
        br = new BufferedReader( new InputStreamReader( new FileInputStream(
                nodeFile ) ) );
        String strLine;
        // Read File Line By Line
        if ( ( strLine = br.readLine() ) != null )
        {
            processHeader( strLine );
        }
        else
        {
            System.out.println( "no records found in " + nodeFile );
        }
    }

    private void processHeader( String strLine )
    {
        fields = strLine.split( "," );
    }

}
