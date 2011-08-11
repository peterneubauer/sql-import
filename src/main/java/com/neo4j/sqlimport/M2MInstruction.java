package com.neo4j.sqlimport;

import java.util.Map;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

public class M2MInstruction extends LinkInstruction {

	private final String fromAggregationName;
	private final Field fromNodeIdField;
	private final String fromNodeIndexName;
	private final Field toNodeIdField;
	private final String toNodeIndexName;
	private final RelationshipType relationshipType;

	public M2MInstruction(String fromAggregationName, Field fromIdField,
			String fromNodeIdIndexName, Field toIdField,
			String toNodeIdIndexName, RelationshipType relationshipType) {
		this.fromAggregationName = fromAggregationName;
		this.fromNodeIdField = fromIdField;
		this.fromNodeIndexName = fromNodeIdIndexName;
		this.toNodeIdField = toIdField;
		this.toNodeIndexName = toNodeIdIndexName;
		this.relationshipType = relationshipType;
	}

	@Override
	public void execute(BatchInserterImpl neo,
	        BatchInserterIndexProvider indexService) {
		long start = System.currentTimeMillis();
		long aggregationNodeId = SQLImporter.getSubRefNode(fromAggregationName,
				neo);

		int linkCount = 0;
		if (aggregationNodeId < 0) {
			System.out.println("could not find aggregation node for "
					+ fromAggregationName);
		} else {
			for (SimpleRelationship rel : neo
					.getRelationships(aggregationNodeId)) {
				// don't traverse subref rel
				if (rel.getType().name().equals(Relationships.IS_A.name())) {
					long linkNodeId = rel.getStartNode();
					Map<String, Object> linkNodeProperties = neo
							.getNodeProperties(linkNodeId);
					long fromNodeId = getNodeIdFromIndex(fromNodeIdField,
							fromNodeIndexName, indexService, linkNodeId,
							linkNodeProperties);
					long toNodeId = getNodeIdFromIndex(toNodeIdField,
							toNodeIndexName, indexService, linkNodeId,
							linkNodeProperties);
					linkNodeProperties.remove( fromNodeIdField.name );
                    linkNodeProperties.remove( toNodeIdField.name );
					// link
					if (fromNodeId > 0 && toNodeId > 0) {
						neo.createRelationship(fromNodeId, toNodeId,
								relationshipType, linkNodeProperties);
						linkCount++;
					}

				}
			}
		}
		System.out.println("created " + linkCount + " relationships between "
				+ fromAggregationName + ":" + fromNodeIdField.name + "--"
				+ relationshipType.name() + "->" + fromAggregationName + ":"
				+ toNodeIdField.name + " in "
				+ (System.currentTimeMillis() - start) + "ms");

	}

	private long getNodeIdFromIndex(Field fromNodeIdField,
			String fromNodeIndexName, BatchInserterIndexProvider indexService,
			long linkNodeId, Map<String, Object> fromNodeProperties) {
		long fromNodeId = -1;
		if (fromNodeProperties.containsKey(fromNodeIdField.name)) {
			Object fromNodeIdProp = fromNodeProperties
					.get(fromNodeIdField.name);
			fromNodeId = indexService.nodeIndex( fromNodeIndexName, MapUtil.stringMap( "type", "exact" ) ).get(
			        fromNodeIndexName, fromNodeIdProp).getSingle();
			if (fromNodeId > 0) {
			} else {
				System.out.println("could not find source node for value "
						+ fromNodeIdProp + " in index " + fromNodeIndexName);
			}
		} else {
			System.out.println("could not find prorperty "
					+ fromNodeIdField.name + " on node " + linkNodeId);
		}
		return fromNodeId;
	}

}
