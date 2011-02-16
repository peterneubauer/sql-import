package com.neo4j.sqlimport;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InsertTest {

	private static final String SQL_FILE = "example.sql";
	private static final String TAG_RELATIONS = "tag_relations";
	private static final String TAG_HIERARCHIES = "tag_hierarchies";
	private static final String TAGGINGS = "taggings";
	private static final String TAGS = "tags";
	private static final String AUTHORSHIPS = "authorships";
	private static final String AUTHORS = "authors";
	private static final String BOOKS = "books";
	private static final Field ISBN_ID_FIELD = new Field("id",
			Field.Type.INTEGER_AS_STRING);
	private static final StringField NAME_FIELD = new StringField("name");
	private static final String ID = "id";
	private static final String AUTHOR_ID = "author_id";
	private static final IntField AUTHOR_ID_FIELD = new IntField(AUTHOR_ID);
	private static final String BOOK_ID = "book_id";
	private static final Field FIELD_ID = new IntField(ID);
	private static final Field TITLE_FIELD = new StringField("title");
	private static final Field DESC_FIELD = new StringField("desc");
	private static final Field BOOK_ID_FIELD = new IntField("taggable_id");
	private static final Field TAG_ID_FIELD = new IntField("tag_id");
	private static final Field PARENT_ID_FIELD = new IntField("parent_id");
	private static final Field TO_ID_FIELD = new IntField("to_id");
	private static final Field FROM_ID_FIELD = new IntField("from_id");
	private static final String ISBN_ID = "isbn_id";
	private static final String ISBNS = "isbns";
	private static final String TAG_ID = "tag_id";
	private SQLImporter importer;

	@Before
	public void setUp() {
		importer = new SQLImporter();
		importer.deleteDB();
	}

	@Test
	public void importAll() {
		importBooks();
		importISBNs();
		importAuthors();
		importAuthorships();
		importTags();
		importTaggings();
		importTaggingHierarchy();
		importTagRelations();
		createRelationsships();
		importer.startImport(SQL_FILE);
	}

	public void importBooks() {
		Field[] fields = { FIELD_ID, TITLE_FIELD };
		Map<Field, String> indexes = new HashMap<Field, String>();
		indexes.put(FIELD_ID, BOOK_ID);
		importer.addInstruction(BOOKS, "Book", fields, indexes);
	}

	public void importISBNs() {
		Field[] fields = { ISBN_ID_FIELD, BOOK_ID_FIELD,
				new StringField("isbn"), new StringField("comment") };
		Map<Field, String> indexes = new HashMap<Field, String>();
		indexes.put(ISBN_ID_FIELD, ISBN_ID);
		importer.addInstruction(ISBNS, "Isbn", fields, indexes);
	}

	public void importAuthors() {
		Field[] fields = { FIELD_ID, NAME_FIELD };
		Map<Field, String> indexes = new HashMap<Field, String>();
		indexes.put(FIELD_ID, AUTHOR_ID);
		importer.addInstruction(AUTHORS, "Author", fields, indexes);
	}
	public void importAuthorships() {
		Field[] fields = { FIELD_ID, AUTHOR_ID_FIELD,
				BOOK_ID_FIELD };
		Map<Field, String> indexes = new HashMap<Field, String>();
		// indexes.put(ID, AUTHORSHIP_ID);

		importer.addInstruction(AUTHORSHIPS, "Authorship", fields, indexes);
	}
	private void importTags() {
		Field[] fields = { FIELD_ID, NAME_FIELD };

		Map<Field, String> indexes = new HashMap<Field, String>();
		indexes.put(FIELD_ID, TAG_ID);

		importer.addInstruction(TAGS, "Tag", fields, indexes);
	}

	private void importTaggings() {
		Field[] fields = { FIELD_ID, TAG_ID_FIELD, BOOK_ID_FIELD };
		Map<Field, String> indexes = new HashMap<Field, String>();

		importer.addInstruction(TAGGINGS, "tagging", fields, indexes);
	}

	private void importTaggingHierarchy() {
		Field[] fields = { FIELD_ID, TAG_ID_FIELD, PARENT_ID_FIELD };
		Map<Field, String> indexes = new HashMap<Field, String>();
		// indexes.put(ID, JOURNAL_ID);

		importer.addInstruction(TAG_HIERARCHIES, "tag_hierarchy", fields,
				indexes);
	}

	private void importTagRelations() {
		Field[] fields = { FIELD_ID, FROM_ID_FIELD, TO_ID_FIELD, DESC_FIELD };
		Map<Field, String> indexes = new HashMap<Field, String>();
		// indexes.put(ID, JOURNAL_ID);

		importer.addInstruction(TAG_RELATIONS, "tag_relation", fields, indexes);
	}

	private void createRelationsships() {
		importer.addLinkInstruction(new ForeignKeyInstruction(ISBNS,
				BOOK_ID_FIELD, BOOK_ID, TestRelationships.ISBN));
		importer.addLinkInstruction(new ForeignKeyInstruction(AUTHORSHIPS,
				AUTHOR_ID_FIELD, AUTHOR_ID));
		importer.addLinkInstruction(new ForeignKeyInstruction(AUTHORSHIPS,
				BOOK_ID_FIELD, BOOK_ID));
		importer.addLinkInstruction(new M2MInstruction(TAG_HIERARCHIES,
				TAG_ID_FIELD, TAG_ID, PARENT_ID_FIELD, TAG_ID,
				TestRelationships.PARENT_TAG));
		importer.addLinkInstruction(new M2MInstruction(TAG_RELATIONS,
				FROM_ID_FIELD, TAG_ID, TO_ID_FIELD, TAG_ID,
				TestRelationships.TAG_RELATION));
	}

	@After
	public void shutdown() throws Exception {
	}
}
