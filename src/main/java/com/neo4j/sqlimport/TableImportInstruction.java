package com.neo4j.sqlimport;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

public class TableImportInstruction extends ImportInstruction
{

    private final String aggregationNodeName;
    private final Map<Field, String> indexes;
    private final Map<Field, String> foreignKeys;
    private boolean createSubrefNodes = true;

    public TableImportInstruction( String aggregationNodeName,
            String tableName, Field[] names, Map<Field, String> indexes,
            Map<Field, String> foreignKeys, boolean createSubrefNodes )
    {
        super( names, "INSERT INTO \"" + tableName + "\" VALUES" );
        this.aggregationNodeName = aggregationNodeName;
        this.indexes = indexes;
        this.foreignKeys = foreignKeys;
        this.createSubrefNodes = createSubrefNodes;
    }

    public TableImportInstruction( String aggregationNodeName,
            String tableName, Field[] names, Map<Field, String> indexes )
    {
        this( aggregationNodeName, tableName, names, indexes,
                new HashMap<Field, String>(), true );
    }

    public String getAggregationNodeName()
    {
        return aggregationNodeName;
    }

    public Map<Field, String> getIndexes()
    {
        return indexes;
    }

    @Override
    void createData( BatchInserter neo,
            BatchInserterIndexProvider indexProvider,
            HashMap<String, Object> values )
    {
        List<Long> nodeIds = new LinkedList<Long>();
        long nodeId = neo.createNode( values );
        nodeIds.add( nodeId );
        if ( createSubrefNodes )
        {
            neo.createRelationship( nodeId,
                    SQLImporter.getOrCreateSubRefNode( getAggregationNodeName(), neo ),
                    Relationships.IS_A, new HashMap<String, Object>() );
        }
        // index the necessary properties
        Map<Field, String> indexes = getIndexes();
        for ( Field indexField : indexes.keySet() )
        {
            try
            {
                String indexName = indexes.get( indexField );
                indexProvider.nodeIndex( indexName,
                        MapUtil.stringMap( "type", "exact" ) ).add( nodeId,
                        MapUtil.map( indexName, values.get( indexField.name ) ) );
            }
            catch ( NumberFormatException nfe )
            {
                nfe.printStackTrace();
            }
        }

        // todo enforce foreign keys
        for ( Field foreignField : foreignKeys.keySet() )
        {
            try
            {
                String indexName = foreignKeys.get( foreignField );
                BatchInserterIndex index = indexProvider.nodeIndex( indexName,
                        MapUtil.stringMap( "type", "exact" ) );
                neo.createRelationship(
                        nodeId,
                        index.get( indexName, values.get( foreignField.name ) ).getSingle(),
                        DynamicRelationshipType.withName( foreignField.name
                                                          + "_LINKED_TO" ),
                        new HashMap<String, Object>() );
            }
            catch ( NumberFormatException nfe )
            {
                nfe.printStackTrace();
            }
        }

    }

}
