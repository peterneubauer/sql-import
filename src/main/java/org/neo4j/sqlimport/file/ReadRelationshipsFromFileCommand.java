package org.neo4j.sqlimport.file;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class ReadRelationshipsFromFileCommand extends ReadNodesFromFileCommand
{

    private final boolean nodeIdsAreNeo4jIds;

    public ReadRelationshipsFromFileCommand( File relFile,
            boolean nodeIdsAreNeo4jIds, String... indexFields )
    {
        super(relFile, true, indexFields);
        this.nodeIdsAreNeo4jIds = nodeIdsAreNeo4jIds;
    }


    protected void processRecord( BatchInserterImpl neo4j,
            LuceneIndexBatchInserterImpl index, Map<String, Object> properties,
            String[] values )
    {
        if ( nodeIdsAreNeo4jIds )
        {
            neo4j.createRelationship( Integer.parseInt( values[0] ), Integer.parseInt( values[1] ), DynamicRelationshipType.withName(values[2]), properties );
        }
        else
        {
            neo4j.createNode( properties );
        }
        //index, not supported yet
//        for(String key : properties.keySet()) {
//            if(indexFields.contains(key)) {
//                index.index(nodeId, key, properties.get(key));
//            }
//        }
    }


}
