package com.neo4j.sqlimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

import com.neo4j.sqlimport.Field.Type;

public class SQLImporter
{

    private static String STORE_DIR = "target/db";
    private ArrayList<ImportInstruction> instructions = new ArrayList<ImportInstruction>();
    private ArrayList<AutoImportInstruction> autoImportinstructions = new ArrayList<AutoImportInstruction>();
    private BatchInserterIndexProvider indexProvider;
    private BatchInserterImpl neo;
    private int nodecount = 0;
    private int oldcount = 0;
    private ArrayList<LinkInstruction> linkInstructions = new ArrayList<LinkInstruction>();
    private Map<String, IndexInstruction> indexInstructions = new HashMap<String, IndexInstruction>();
    private static String insert = "insert into";

    protected static final String NAME = "name";

    public SQLImporter()
    {
        this( "target/db" );
    }

    public void deleteDB()
    {
        File db = new File( STORE_DIR );
        deleteDirectory( db );
    }

    public SQLImporter( String dbDirectory )
    {

        STORE_DIR = dbDirectory;
    }

    public void addImportInstruction( ImportInstruction instruction )
    {
        instructions.add( instruction );

    }

    public void optimizeIndex()
    {
        // indexService.optimize();
        System.out.println( "index optimized" );
    }

    private boolean deleteDirectory( File path )
    {
        if ( path.exists() )
        {
            File[] files = path.listFiles();
            for ( int i = 0; i < files.length; i++ )
            {
                if ( files[i].isDirectory() )
                {
                    deleteDirectory( files[i] );
                }
                else
                {
                    files[i].delete();
                }
            }
        }
        return ( path.delete() );
    }

    public void startImport( String sqlFile )
    {
        long start = System.currentTimeMillis();

        neo = new BatchInserterImpl( STORE_DIR );
        indexProvider = new LuceneBatchInserterIndexProvider( neo );
        int nodecount = 0;
        try
        {

//            createSubRefNodes( instructions );
            BufferedReader br = new BufferedReader( new FileReader( sqlFile ) );

            // replace ,NULL, with ,'',
            int i = 0;
            String line = getNextLine( br );
            while ( line != null )
            {
                if ( line.contains( "NULL" ) )
                {
                    line = line.replaceAll( "NULL", "''" );
                    line = line.replaceAll( "''''", "'" );
                }
                for ( final ImportInstruction instruction : instructions )
                {
                    // first, see to the line start
                    if ( line.startsWith( instruction.getStatementStart() ) )
                    {

                        int length = instruction.getStatementStart().length();
                        String substring = line.substring( length );
                        final String[] values = instruction.parse( substring );
                        try
                        {
                            Field[] fields = instruction.getNames();
                            // System.out.println(line);
                            int nrOfFields = fields.length;
                            final HashMap<String, Object> record = new HashMap<String, Object>();
                            for ( int k = 0; k < nrOfFields; k++ )
                            {
                                String nextToken = values[k];
                                // integers
                                Type type = fields[k].type;
                                if ( ( type == Type.INTEGER || type == Type.INTEGER_AS_STRING )
                                     && !nextToken.equals( "''" ) )
                                {
                                    try
                                    {

                                        int value = Integer.parseInt( nextToken.trim() );
                                        record.put( fields[k].name, value );
                                    }
                                    catch ( Exception e )
                                    {
                                        System.out.println( "could not parse "
                                                            + nextToken + " "
                                                            + e );
                                    }
                                }

                                if ( type == Type.STRING )
                                {
                                    record.put( fields[k].name, nextToken );
                                }

                            }
                            nodecount++;

                            instruction.createData( neo, indexProvider, record );

                            if ( nodecount - 1000 == oldcount )
                            {
                                System.out.println( "." );
                                oldcount = nodecount;
                            }


                        }
                        catch ( Exception e )
                        {
                            e.printStackTrace( System.err );
                        }

                    }
                }
                line = getNextLine( br );
                nodecount++;
                i++;

            }
            br.close();
        }
        catch ( FileNotFoundException e )
        {
            e.printStackTrace();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        System.out.println( "submitted " + nodecount + "nodes in "
                            + ( System.currentTimeMillis() - start ) / 1000
                            + "s" );
//        optimizeIndex();
        startLinking();
//        neo.shutdown();

    }

    public void startLinking()
    {
//        startup();
        for ( IndexInstruction instruction : indexInstructions.values() )
        {
            instruction.execute( neo, indexProvider );
        }
        for ( LinkInstruction instruction : linkInstructions )
        {
            instruction.execute( neo, indexProvider );
        }
//        shutdown();
    }

    public void shutdown()
    {
        indexProvider.shutdown();
        neo.shutdown();

    }

    private void startup()
    {
        if ( neo == null )
        {
            neo = new BatchInserterImpl( STORE_DIR );
        }
        if ( indexProvider == null )
        {

            indexProvider = new LuceneBatchInserterIndexProvider( neo );
        }

    }

    public static String getNextLine( BufferedReader br )
    {
        try
        {
            String line = br.readLine();
            if ( line != null )
            {
                while ( null != line && !line.endsWith( ";" )
                        && !line.startsWith( "--" ) )
                {
                    String nextLine = br.readLine();
                    line = line.concat( nextLine );
                }
                line.replaceAll( "\n", "" );
                return line;
            }
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

//    private void createNode( TableImportInstruction instruction,
//            HashMap<String, Object> values )
//    {
//        nodecount++;
//        List<Long> nodeIds = new LinkedList<Long>();
//        long nodeId = neo.createNode( values );
//        nodeIds.add( nodeId );
//        neo.createRelationship( nodeId, SQLImporter.getSubRefNode(
//                instruction.getAggregationNodeName(), neo ),
//                Relationships.IS_A, new HashMap<String, Object>() );
//        // index the necessary properties
//        Map<Field, String> indexes = instruction.getIndexes();
//        for ( Field indexField : indexes.keySet() )
//        {
//            try
//            {
//                String indexName = indexes.get( indexField );
//                indexProvider.nodeIndex( indexName,
//                        MapUtil.stringMap( "type", "exact" ) ).add( nodeId,
//                        MapUtil.map( indexName, values.get( indexField.name ) ) );
//            }
//            catch ( NumberFormatException nfe )
//            {
//                nfe.printStackTrace();
//            }
//        }
//
//        if ( nodecount - 1000 == oldcount )
//        {
//            System.out.println( "." );
//            oldcount = nodecount;
//        }
//    }

    public static long getOrCreateSubRefNode( String aggregationNodeName,
            BatchInserter neo )
    {
        String subrefName = "subref_" + aggregationNodeName;
        long aggregationNodeId = -1;
        Iterable<SimpleRelationship> relationships = neo.getRelationships( 0 );
        for ( SimpleRelationship rel : relationships )
        {
            if ( rel.getType().name().equals(
                    DynamicRelationshipType.withName( subrefName ).name() ) )
            {
                aggregationNodeId = rel.getEndNode();

            }
        }
        if (aggregationNodeId == -1) {
            aggregationNodeId = createSubrefNode( neo, aggregationNodeName );
        }
        return aggregationNodeId;
    }

    public void addLinkInstruction( LinkInstruction foreignKeyInstruction )
    {
        linkInstructions.add( foreignKeyInstruction );

    }

    private void createSubRefNodes( ArrayList<ImportInstruction> instructions2 )
    {
        for ( ImportInstruction ins : instructions2 )
        {
            if(ins instanceof TableImportInstruction ) {
                
                createSubrefNode( neo, ((TableImportInstruction)ins).getAggregationNodeName() );
            }
        }

    }

    private static long createSubrefNode( BatchInserter neo,
            String aggregationName )
    {
        HashMap<String, Object> props = new HashMap<String, Object>();
        props.put( NAME, "subref_" + aggregationName );
        long aggregationNodeId = neo.createNode( props );
        System.out.println( aggregationNodeId + ", creating " + aggregationName );
        neo.createRelationship(
                0,
                aggregationNodeId,
                DynamicRelationshipType.withName( "subref_" + aggregationName ),
                null );
        return aggregationNodeId;
    }

    public void autoImport( String sqlFile )
    {
        startup();
        AutoImportInstruction instruction = new AutoImportInstruction( sqlFile );
        autoImportinstructions.add( instruction );
        startImporting();
    }

    static void createNode( Field[] fields, String line,
            BatchInserterImpl neo2, long subrefId )
    {
        String VALUES = "values(";
        int firstComma = line.indexOf( VALUES );
        int secondComma = line.toLowerCase().indexOf( ");" );
        String values = line.substring( firstComma + VALUES.length(),
                secondComma + 2 ).trim();
        values = values.substring( 0, values.length() ).trim();
        Map<String, Object> result = new HashMap<String, Object>();
        for ( int i = 0; i < fields.length; i++ )
        {
            Field field = fields[i];
            boolean lastField = ( i == fields.length - 1 );
            if ( values.startsWith( "'" ) )
            {
                String value = values.substring( 1,
                        values.indexOf( lastField ? "');" : "'," ) );
                result.put( field.name, value );
                if ( !lastField )
                {
                    values = values.substring( value.length() + 3 );
                }
            }
            else
            {
                String value = values.substring( 0,
                        values.indexOf( lastField ? ");" : "," ) );
                try
                {
                    result.put( field.name, Integer.parseInt( value ) );
                }
                catch ( NumberFormatException e )
                {
                    // this was a string after all
                    result.put( field.name, value );
                }
                if ( !lastField )
                {

                    values = values.substring( value.length() + 1 );
                }
            }
        }
        long newNode = neo2.createNode( result );
        neo2.createRelationship( newNode, subrefId, Relationships.IS_A, null );
    }

    static Field[] getFieldsForLine( String line )
    {
        ArrayList<Field> fields = new ArrayList<Field>();
        Field[] result = new Field[0];
        int firstComma = line.indexOf( "(" );
        int secondComma = line.toLowerCase().indexOf( "values(" );
        String values = line.substring( firstComma, secondComma ).trim();
        values = values.substring( 1, values.length() - 1 ).trim();
        StringTokenizer fieldNames = new StringTokenizer( values, "," );
        while ( fieldNames.hasMoreTokens() )
        {
            fields.add( new Field( fieldNames.nextToken(), Field.Type.STRING ) );
        }
        return fields.toArray( result );
    }

    public static long cerateOrFindSubrefNode( String tableName,
            BatchInserterImpl neo )
    {
        long subRefNode = getOrCreateSubRefNode( tableName, neo );
        if ( subRefNode < 0 )
        {
            subRefNode = createSubrefNode( neo, tableName );
        }
        return subRefNode;
    }

    static String getTableNameFromInsertStatement( String line )
    {
        if ( line == null )
        {
            return null;
        }
        String name = line.substring( insert.length(), line.indexOf( "(" ) ).trim();
        return name;
    }

    static String getNextInsertLine( BufferedReader br )
    {
        String result = getNextLine( br );
        while ( result != null && !result.toLowerCase().startsWith( insert ) )
        {
            result = getNextLine( br );
        }
        if ( result == null )
        {
            return null;
        }
        return result;
    }

    public void autoLink( String fromAggregationName, String fromField,
            String toAggregationName, String toIdField, String relationshipName )
    {
        String createindexName = createindexName( toAggregationName, toIdField );
        this.addIndexInstruction( new IndexInstruction( toAggregationName,
                toIdField ) );

//        this.addLinkInstruction( new ForeignKeyInstruction(
//                fromAggregationName, new StringField( fromField ),
//                createindexName,
//                DynamicRelationshipType.withName( relationshipName ) ) );
    }

    /**
     * Linking table representation as network
     * 
     * @param aggregationName the node type holding both ID fields
     * @param fromAggregationName
     * @param fromIdField
     * @param toAggregationName
     * @param toIdField
     * @param RelationshipName
     */
    public void autoLink( String aggregationName, String fromAggregationName,
            String fromIdField, String toAggregationName, String toIdField,
            String RelationshipName )
    {
        this.addIndexInstruction( new IndexInstruction( fromAggregationName,
                fromIdField ) );
        // create the index on the target nodes
        this.addIndexInstruction( new IndexInstruction( toAggregationName,
                toIdField ) );
        this.addLinkInstruction( new M2MInstruction( aggregationName,
                new StringField( fromIdField ), createindexName(
                        fromAggregationName, fromIdField ), new StringField(
                        toIdField ), createindexName( toAggregationName,
                        toIdField ),
                DynamicRelationshipType.withName( RelationshipName ) ) );
    }

    private void addIndexInstruction( IndexInstruction indexInstruction )
    {
        indexInstructions.put( indexInstruction.getIndexName(),
                indexInstruction );
    }

    public static String createindexName( String toAggregationName,
            String toIdField )
    {
        return toAggregationName + ":" + toIdField;
    }

    public void startImporting()
    {
//        startup();

        for ( AutoImportInstruction instruction : autoImportinstructions )
        {
            instruction.execute( neo, indexProvider );

        }

//        shutdown();

    }
}
