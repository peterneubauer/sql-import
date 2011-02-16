package org.neo4j.sqlimport.file;

import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public interface FileImportCommand {

	void execute(BatchInserterImpl neo, LuceneIndexBatchInserterImpl indexService, int stepSize);
}
