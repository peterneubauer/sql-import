package org.neo4j.sqlimport.file;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

public class FileImportTest
{

    private static final String TARGET_DIR = "target/neo4j";
    private BatchInserterImpl neo4j;
    private BatchInserterIndexProvider indexProvider;
    private File nodeFile;
    private File relFile;

    public static void deleteFileOrDirectory( File file )
    {
        if ( file.exists() )
        {
            if ( file.isDirectory() )
            {
                for ( File child : file.listFiles() )
                {
                    deleteFileOrDirectory( child );
                }
            }
            file.delete();
        }
    }

    @Before
    public void setUp() throws Exception
    {
        deleteFileOrDirectory( new File( TARGET_DIR ) );
        neo4j = new BatchInserterImpl( TARGET_DIR );
        indexProvider = new LuceneBatchInserterIndexProvider( neo4j );
        nodeFile = new File( "nodes.txt" );
        relFile = new File( "rels.txt" );
    }

    @Test
    public void importFromNodeFileWithIds()
    {
        FileImportCommand readNodes = new ReadNodesFromFileCommand( nodeFile,
                true, "name" );
        readNodes.execute( neo4j, indexProvider, 1 );
        assertNotNull( neo4j.getNodeProperties( 10 ) );
        assertNotNull( indexProvider.nodeIndex( "name", MapUtil.stringMap( "type", "exact" )).get( "name", "peter" ).iterator().next() );
    }

    @Test
    public void importFromNodeFileWithNeo4jIds()
    {
        FileImportCommand readNodes = new ReadNodesFromFileCommand( nodeFile,
                false, "name" );
        readNodes.execute( neo4j, indexProvider, 1 );
        assertTrue( neo4j.getNodeProperties( 0 ).isEmpty() );
        assertFalse( neo4j.getNodeProperties( 1 ).isEmpty() );
        assertFalse( neo4j.getNodeProperties( 2 ).isEmpty() );
        assertNotNull( indexProvider.nodeIndex( "name", MapUtil.stringMap( "type", "exact" )).get( "name", "peter" ).iterator().next() );
    }

    @Test
    public void importFromRelsWithNeo4jIds()
    {
        FileImportCommand readNodes = new ReadNodesFromFileCommand( nodeFile,
                true, "name" );
        readNodes.execute( neo4j, indexProvider, 1 );
        FileImportCommand readRels = new ReadRelationshipsFromFileCommand(
                relFile, true, "name" );
        readRels.execute( neo4j, indexProvider, 1 );
        assertNotNull( neo4j.getNodeProperties( 1 ) );
        assertNotNull( neo4j.getNodeProperties( 10 ) );
        assertNotNull( indexProvider.nodeIndex( "name", MapUtil.stringMap( "type", "exact" )).get( "name", "peter" ).iterator().next() );
        assertTrue( neo4j.getRelationships( 10 ).iterator().hasNext() );
        assertTrue( neo4j.getRelationships( 1 ).iterator().hasNext() );
        SimpleRelationship relationship = neo4j.getRelationships( 1 ).iterator().next();
        assertTrue( relationship.getStartNode() == 10 );
        assertTrue( relationship.getType().name().equals( "OWNS" ) );
        assertTrue( neo4j.getRelationshipProperties( relationship.getId() ).get(
                "since" ).equals( "2000" ) );
    }

    @After
    public void tearDown() throws Exception
    {
        indexProvider.shutdown();
        neo4j.shutdown();
    }

}
