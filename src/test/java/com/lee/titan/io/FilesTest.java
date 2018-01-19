package com.lee.titan.io;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public class FilesTest {
	
	
	@Test
	public void readAllLinesTest() throws IOException {
		
		List<String> lines = Files.readAllLines(new File("conf/graph.schema").toPath(), Charset.forName("utf-8"));
		
		System.out.println(lines.size());
		
		for (String line : lines) {
			System.out.println(line.trim().length());
		}
	}
	
	@Test
	public void readAllBytesTest() throws UnsupportedEncodingException, IOException {
		
		String schema = new String(Files.readAllBytes(new File("conf/graph.schema").toPath()), "utf-8");
		
		String vertexLabel = schema.substring(schema.indexOf("vertexLabel") + 12, schema.indexOf("edgeLabel"));
		
		String edgeLabel = schema.substring(schema.indexOf("edgeLabel") + 10, schema.indexOf("property")).trim();
		
		String property = schema.substring(schema.indexOf("property") + 9).trim();
		
		System.out.println(vertexLabel);
		System.out.println(edgeLabel);
		System.out.println(property);
	}
	
	@Test
	public void splitTest() {
		String str = "股东,";
		String[] ss = str.split(",");
		System.out.println(ss.length);
	}
	
	@Test
	public void listTest() {
		List<String> list = new ArrayList<>();
		list.add("a");
		list.add("b");
		list.add("c");
		list.add("d");
		list.add("e");
		
		System.out.println(list.toArray(new String[5]));
	}
	
	@Test
	public void removeTest() {
		List<String> list = new ArrayList<>();
		list.add("hello");
		list.add("world");
		list.add("123");
		list.add("abc");
		
		System.out.println(list);
		Iterator<String> iterator = list.iterator();
		while (iterator.hasNext()) {
			String str = iterator.next();
			if (!"123".equals(str)) iterator.remove();
		}
		System.out.println("-------------");
		System.out.println(list);
	}
}
