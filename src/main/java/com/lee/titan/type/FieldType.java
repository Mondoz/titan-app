package com.lee.titan.type;

public enum FieldType {
	
	CHAR("char"),
	STRING("string"),
	BYTE("byte"),
	SHORT("short"),
	INT("int"),
	LONG("long"),
	BIGINT("bigint"),
	FLOAT("float"),
	DOUBLE("double");
	
	private final String alias;
	
	private FieldType(String alias) {
		this.alias = alias;
	}
	
	public String alias() {
		return alias;
	}
	
	public String toString() {
		return alias;
	}
}
