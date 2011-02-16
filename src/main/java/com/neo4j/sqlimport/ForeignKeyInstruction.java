package com.neo4j.sqlimport;

import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;


public class ForeignKeyInstruction extends LinkInstruction {

	private final String aggregationName;
	private final Field fromIdField;
	private final String toIdIndexName;
	private final RelationshipType relationshipType;

	public ForeignKeyInstruction(String aggregationName, Field fromIdField,
			String toIdIndexName, RelationshipType relationshipType) {
		this.aggregationName = aggregationName;
		this.fromIdField = fromIdField;
		this.toIdIndexName = toIdIndexName;
		this.relationshipType = relationshipType;
	}

	public ForeignKeyInstruction(String aggregationName, Field fromIdField,
			String toIdIndexName) {
		this(aggregationName, fromIdField, toIdIndexName,
				DynamicRelationshipType.withName("refers_to_" + toIdIndexName));
	}

	public void execute(BatchInserterImpl neo,
			LuceneIndexBatchInserterImpl indexService) {
		long start = System.currentTimeMillis();

		long aggregationNodeId = SQLImporter
				.getSubRefNode(aggregationName, neo);

		int linkCount = 0;
		if (aggregationNodeId < 0) {
			System.out.println("could not find aggregation node for "
					+ aggregationName);
		} else {
			for (SimpleRelationship rel : neo
					.getRelationships(aggregationNodeId)) {
				// don't traverse subref rel
				if (rel.getType().name().equals(Relationships.IS_A.name())) {
					long fromNodeId = rel.getStartNode();
					if(fromNodeId == aggregationNodeId) {
						fromNodeId = rel.getEndNode();
					}
					Map<String, Object> fromNodeProperties = neo
							.getNodeProperties(fromNodeId);
					if (fromNodeProperties.containsKey(fromIdField.name)) {
						Object toNodeIdProp = fromNodeProperties
								.get(fromIdField.name);
						long toNodeId = indexService.getSingleNode(
								toIdIndexName, toNodeIdProp);
						if (toNodeId > 0) {
							neo.createRelationship(fromNodeId, toNodeId,
									relationshipType, null);
							linkCount++;
						} else {
							System.out
									.println("could not find target node for value "
											+ toNodeIdProp
											+ " in index "
											+ toIdIndexName);
						}
					} else {
						System.out.println("could not find prorperty "
								+ fromIdField.name + " on node " + fromNodeId);
					}
				}
			}
		}
		System.out.println("created " + linkCount + " relationships between "
				+ aggregationName + ":" + fromIdField.name + "--"
				+ relationshipType.name() + "->" + toIdIndexName
				+ " index prop in " + (System.currentTimeMillis() - start)
				+ "ms");

	}

}
