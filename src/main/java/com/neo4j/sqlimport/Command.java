package com.neo4j.sqlimport;

import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public interface Command {

	void execute(BatchInserterImpl neo, BatchInserterIndexProvider indexService);
}
