package com.neo4j.sqlimport;

import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class AutoInsertTest {

	private static final String SQL_FILE = "example2.sql";
	private static final String DB_DIR = "target/db";
	private SQLImporter importer;

	@Before
	public void setUp() {
		importer = new SQLImporter(DB_DIR);
		importer.deleteDB();
	}

	@Test
	public void importAll() {
		//import the data
		importer.autoImport(SQL_FILE);
		
		//make the links
		importer.autoLink("Comment","BOOK_ID", "Book", "BOOK_ID", "talks_about");
		importer.autoLink("Author_Book","Author","AUTHOR_ID", "Book", "BOOK_ID", "author_of");
		importer.startLinking();
		//verify the results
		EmbeddedGraphDatabase neo = new EmbeddedGraphDatabase(DB_DIR);
		Transaction tx = neo.beginTx();
		Node referenceNode = neo.getReferenceNode();
		Iterator<Relationship> subrefs = referenceNode.getRelationships().iterator();
		assertTrue(subrefs.hasNext());
		Node pippi = neo.getNodeById(2);
		assertTrue(pippi.hasProperty("BOOK_ID"));
		assertTrue(pippi.hasRelationship(DynamicRelationshipType.withName("talks_about")));
		assertTrue(pippi.hasRelationship(DynamicRelationshipType.withName("author_of")));
		assertTrue(pippi.hasRelationship(Relationships.IS_A));
		Assert.assertEquals("2009-05-13",pippi.getProperty("CREATED_DATE_TIME"));
		Node test = neo.getNodeById(10);
		Assert.assertEquals(" 2009-06-05",test.getProperty("DATE"));
		Assert.assertEquals("some_proc('2009-06-05','RRRR-MM-DD')",test.getProperty("PROC"));
		
		tx.success();
		tx.finish();
		
		
	}

	@After
	public void shutdown() throws Exception {
	}
}
