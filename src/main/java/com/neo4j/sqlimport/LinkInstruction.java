package com.neo4j.sqlimport;

import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public abstract class LinkInstruction {


	public abstract void execute(BatchInserterImpl neo,
	        BatchInserterIndexProvider indexService);
	
	
}
