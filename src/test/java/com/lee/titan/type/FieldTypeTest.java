package com.lee.titan.type;

import org.junit.Test;

public class FieldTypeTest {
	
	@Test
	public void test() {
		FieldType type = FieldType.STRING;
		System.out.println(type.alias());
	}
	
	@Test
	public void equalsTest() {
		FieldType type = FieldType.STRING;
		System.out.println(type == FieldType.STRING);
		System.out.println(type.equals(FieldType.STRING));
	}
	
	@Test
	public void valueOfTest() {
		FieldType s = FieldType.STRING;
		
		FieldType sq = FieldType.valueOf("string".toUpperCase());
		
		System.out.println(s == sq);
	}
}
