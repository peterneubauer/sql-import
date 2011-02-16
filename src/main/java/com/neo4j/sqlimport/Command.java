package com.neo4j.sqlimport;

import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public interface Command {

	void execute(BatchInserterImpl neo, LuceneIndexBatchInserterImpl indexService);
}
