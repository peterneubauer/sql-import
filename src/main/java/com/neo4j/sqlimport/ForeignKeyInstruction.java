package com.neo4j.sqlimport;

import java.util.HashMap;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

public class ForeignKeyInstruction extends ImportInstruction
{

    private final Field fromIdField;
    private final String toIndexName;
    private final RelationshipType relationshipType;
    private final String fromIndexName;
    private final Field toIdField;

    public ForeignKeyInstruction( Field[] names, String tableName,
            Field fromIdField, String fromIndexName, Field toIdField,
            String toIdIndexName, RelationshipType relationshipType )
    {
        super( names, "INSERT INTO \"" + tableName + "\" VALUES" );
        this.fromIndexName = fromIndexName;
        this.toIdField = toIdField;
        this.fromIdField = fromIdField;
        this.toIndexName = toIdIndexName;
        this.relationshipType = relationshipType;
    }

    @Override
    void createData( BatchInserter neo,
            BatchInserterIndexProvider indexProvider,
            HashMap<String, Object> values )
    {
        long fromNodeId = indexProvider.nodeIndex( fromIndexName,
                MapUtil.stringMap( "type", "exact" ) ).get( fromIndexName,
                values.get( fromIdField.name ) ).getSingle();
        long toNodeId = indexProvider.nodeIndex( toIndexName,
                MapUtil.stringMap( "type", "exact" ) ).get( toIndexName,
                values.get( toIdField.name ) ).getSingle();
        values.remove( fromIdField.name );
        values.remove( toIdField.name );
        neo.createRelationship( fromNodeId, toNodeId, relationshipType, values );
    }

}
