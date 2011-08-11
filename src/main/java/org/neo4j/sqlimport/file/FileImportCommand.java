package org.neo4j.sqlimport.file;

import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public interface FileImportCommand {

	void execute(BatchInserterImpl neo, BatchInserterIndexProvider indexService, int stepSize);
}
