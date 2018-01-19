package com.lee.titan.service;

import java.util.List;

import com.lee.titan.service.impl.TitanPublicServiceImpl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

public class TitanPublicServiceTest {
	
	TitanGraph graph;
	TitanPublicService tp;
	GraphTraversalSource g;
	
	@Before
	public void init() {
		graph = TitanFactory.open("conf/ent-graph.properties");
		tp = new TitanPublicServiceImpl(graph);
		g = tp.g();
	}
	
	@Test
	public void verticesTest() {
		List<Vertex> vertices = tp.vertices("name", "黄岩");
		for (Vertex v : vertices) {
			System.out.println(v.label() + " " + v.value("v_id") + " " + v.value("name"));
		}
	}
	
	@Test
	public void recursiveChildTest() {
		List<Vertex> vertices = tp.recursiveChild(262824L);
		for (Vertex v : vertices) {
			System.out.println(v.label() + " " + v.value("v_id") + " " + v.value("name"));
			
			//
			GraphTraversal<Vertex, Vertex> vs = g.V(v.id()).inE("企业标签").outV();
			while (vs.hasNext()) {
				Vertex c = vs.next();
				System.out.println(c.label() + " " + c.value("v_id") + " " + c.value("name"));
			}
		}
	}
	
	@Test
	public void clear() {
		graph.close();
	}
}
