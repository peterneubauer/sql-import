Neo4j SQL Importer
==================
 
This is a first attempt to do a reasonable mapping from SQL dump statements in relational databases into a graph in [Neo4j open source graph database](http://neo4j.org/).

![Books import](https://github.com/peterneubauer/sql-import/raw/master/src/pics/books.png)


Can be imported from SQL like

    BEGIN TRANSACTION;
    CREATE TABLE Book(
          id int primary key,
          title varchar(250),
    );
    INSERT INTO "Book" VALUES(1,,'Pippi 
    Långstrump');
    CREATE TABLE Isbn(
            id int primary key,
            book_id int,
            isbn varchar(20),
            comment varchar(250)
    );
    INSERT INTO "Isbn" VALUES('     1',1,'04712
    17476','');
    CREATE TABLE Author (
            id int primary key,
            name varchar(250)
    );
    INSERT INTO "Author" VALUES(1,'Astrid Lindgren');
    CREATE TABLE Authorship (
            id int primary key,
            author_id int,
            book_id int);
    INSERT INTO "Authorship" VALUES(1,1,1);
    CREATE TABLE tag ("id" INTEGER PRIMARY KEY NOT NULL, "name" varchar(255) DEFAULT NULL);
    INSERT INTO "tag" VALUES(1,'childrens books');
    INSERT INTO "tag" VALUES(2,'books');
    INSERT INTO "tag" VALUES(3,'youth books');
    CREATE TABLE tagging ("id" INTEGER PRIMARY KEY NOT NULL, "tag_id" integer DEFAULT NULL, "book_id" integer DEFAULT NULL);
    INSERT INTO "tagging" VALUES(1,1,1);
    CREATE TABLE tag_hierarchy ("id" INTEGER PRIMARY KEY NOT NULL, "tag_id" integer DEFAULT NULL, "parent_id" integer DEFAULT NULL);
    #INSERT INTO "tag_hierarchy" VALUES(1,2,NULL);
    INSERT INTO "tag_hierarchy" VALUES(2,1,2);
    CREATE TABLE tag_relation ("id" INTEGER PRIMARY KEY NOT NULL, "from_id" integer DEFAULT NULL, "to_id" integer DEFAULT NULL, "desc" varchar(255) DEFAULT NULL);
    INSERT INTO "tag_relation" VALUES(1,1,3,'same genre');
    COMMIT;