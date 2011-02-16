--Comment

Insert into Book
(BOOK_ID,TITLE,CREATED_DATE_TIME) values
(1,'Pippi Långstrump','2009-05-13');
Insert into Author
(AUTHOR_ID,NAME) values
(1,'Astrid Lindgren');
Insert into Author_Book
(AUTHOR_ID,BOOK_ID) values
(1,1);
Insert into Comment
(ID,BOOK_ID,CONTENT) values
(1,1,'best childrens book ever!');


--Comment
Insert into TEST
(ID,DATE,PROC) values
(132164, 2009-06-
05,some_proc('2009-06-
05','RRRR-MM-DD'));