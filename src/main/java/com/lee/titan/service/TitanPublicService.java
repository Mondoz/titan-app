package com.lee.titan.service;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface TitanPublicService {
	//
	GraphTraversalSource g();
	//
	Vertex id(long id);
	
	//
	Vertex vid(long vid);
	
	//
	List<Vertex> vertices(String vertexLabel, Object... keyValues);
	
	//
	List<Vertex> out(long vid, int vtype, String name, String edgeLabel);
	//
	List<Vertex> recursiveChild(long vid);
}
