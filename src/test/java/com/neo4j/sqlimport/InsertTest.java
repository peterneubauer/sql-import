package com.neo4j.sqlimport;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InsertTest
{

    private static final String SQL_FILE = "example.sql";
    private static final String TAG_RELATIONS = "tag_relations";
    private static final String TAG_HIERARCHIES = "tag_hierarchies";
    private static final String TAGGINGS = "taggings";
    private static final String TAGS = "tags";
    private static final String AUTHORSHIPS = "authorships";
    private static final String AUTHORS = "authors";
    private static final String BOOKS = "books";
    private static final Field ISBN_ID_FIELD = new Field( "id",
            Field.Type.INTEGER_AS_STRING );
    private static final StringField NAME_FIELD = new StringField( "name" );
    private static final String ID = "id";
    private static final String AUTHOR_ID = "author_id";
    private static final IntField AUTHOR_ID_FIELD = new IntField( AUTHOR_ID );
    private static final String BOOK_ID = "book_id";
    private static final Field FIELD_ID = new IntField( ID );
    private static final Field TITLE_FIELD = new StringField( "title" );
    private static final Field DESC_FIELD = new StringField( "desc" );
    private static final Field BOOK_ID_FIELD = new IntField( "book_id" );
    private static final Field TAG_ID_FIELD = new IntField( "tag_id" );
    private static final Field PARENT_ID_FIELD = new IntField( "parent_id" );
    private static final Field TO_ID_FIELD = new IntField( "to_id" );
    private static final Field FROM_ID_FIELD = new IntField( "from_id" );
    private static final String ISBN_ID = "isbn_id";
    private static final String ISBNS = "isbns";
    private static final String TAG_ID = "tag_id";
    private SQLImporter importer;

    @Before
    public void setUp()
    {
        importer = new SQLImporter( "target/books" );
        importer.deleteDB();
    }

    @Test
    public void importAll()
    {
        importBooks();
        importISBNs();
        importAuthors();
        importAuthorships();
        importTags();
        importTaggings();
        importTaggingHierarchy();
        importTagRelations();
        importer.startImport( SQL_FILE );
    }

    public void importBooks()
    {
        Field[] fields = { FIELD_ID, TITLE_FIELD };
        Map<Field, String> indexes = new HashMap<Field, String>();
        indexes.put( FIELD_ID, BOOK_ID );
        TableImportInstruction instruction = new TableImportInstruction( BOOKS,
                "Book", fields, indexes );
        importer.addImportInstruction( instruction );
    }

    public void importISBNs()
    {
        Field[] fields = { ISBN_ID_FIELD, BOOK_ID_FIELD,
                new StringField( "isbn" ), new StringField( "comment" ) };
        Map<Field, String> indexes = new HashMap<Field, String>();
        Map<Field, String> foreignKeys = new HashMap<Field, String>();
        foreignKeys.put( BOOK_ID_FIELD, BOOK_ID );
        indexes.put( ISBN_ID_FIELD, ISBN_ID );
        TableImportInstruction instruction = new TableImportInstruction( ISBNS,
                "Isbn", fields, indexes, foreignKeys );
        importer.addImportInstruction( instruction );
    }

    public void importAuthors()
    {
        Field[] fields = { FIELD_ID, NAME_FIELD };
        Map<Field, String> indexes = new HashMap<Field, String>();
        indexes.put( FIELD_ID, AUTHOR_ID );
        TableImportInstruction instruction = new TableImportInstruction(
                AUTHORS, "Author", fields, indexes );
        importer.addImportInstruction( instruction );
    }

    public void importAuthorships()
    {
        Field[] fields = { FIELD_ID, AUTHOR_ID_FIELD, BOOK_ID_FIELD };
        importer.addImportInstruction( new ForeignKeyInstruction( fields, "Authorship",
                AUTHOR_ID_FIELD, AUTHOR_ID, BOOK_ID_FIELD, BOOK_ID,
                TestRelationships.AUTHOR ) );
    }

    private void importTags()
    {
        Field[] fields = { FIELD_ID, NAME_FIELD };

        Map<Field, String> indexes = new HashMap<Field, String>();
        indexes.put( FIELD_ID, TAG_ID );
        TableImportInstruction instruction = new TableImportInstruction( TAGS,
                "tag", fields, indexes );
        importer.addImportInstruction( instruction );
    }

    private void importTaggings()
    {
        Field[] fields = { FIELD_ID, TAG_ID_FIELD, BOOK_ID_FIELD };

        importer.addImportInstruction( new ForeignKeyInstruction( fields, "tagging",
                TAG_ID_FIELD, TAG_ID, BOOK_ID_FIELD, BOOK_ID,
                TestRelationships.TAGGING ) );
    }

    private void importTaggingHierarchy()
    {
        Field[] fields = { FIELD_ID, TAG_ID_FIELD, PARENT_ID_FIELD };
        importer.addImportInstruction( new ForeignKeyInstruction( fields, "tag_hierarchy",
                TAG_ID_FIELD, TAG_ID, PARENT_ID_FIELD, TAG_ID,
                TestRelationships.PARENT_TAG ) );
    }

    private void importTagRelations()
    {
        Field[] fields = { FIELD_ID, FROM_ID_FIELD, TO_ID_FIELD, DESC_FIELD };
        importer.addImportInstruction( new ForeignKeyInstruction( fields, "tag_relation",
                FROM_ID_FIELD, TAG_ID, TO_ID_FIELD, TAG_ID,
                TestRelationships.TAG_RELATION) );
    }


    @After
    public void shutdown() throws Exception
    {
        importer.shutdown();
    }
}
