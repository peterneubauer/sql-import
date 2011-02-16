package com.neo4j.sqlimport;

public class Field {
	final String name;
	final Type type;


	public Field(String name, Type type) {
		this.name = name;
		this.type = type;
	}

	public enum Type {
		INTEGER, STRING, INTEGER_AS_STRING;
	}
}
