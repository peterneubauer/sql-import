package com.neo4j.sqlimport;

import java.util.Map;

import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

public class IndexInstruction implements Command {


	private final String toIdField;
	private final String toAggregationName;
	private String createindexName;

	public IndexInstruction(String toAggregationName, String toIdField) {
				this.toAggregationName = toAggregationName;
				this.toIdField = toIdField;
				createindexName = SQLImporter.createindexName(toAggregationName, toIdField);
	}

	public void execute(BatchInserterImpl neo,
			LuceneIndexBatchInserterImpl indexService) {
		System.out.println("starting indexing " + createindexName);
			for (SimpleRelationship rel : neo.getRelationships(SQLImporter.getSubRefNode(
					toAggregationName, neo))) {
				if (rel.getType().name().equals(Relationships.IS_A.name())) {
					Map<String, Object> nodeProperties = neo.getNodeProperties(rel
							.getStartNode());
					indexService.index(rel.getStartNode(), createindexName, nodeProperties
							.get(toIdField));
				}
			}


	}

	public String getIndexName() {
		return createindexName;
	}

}
