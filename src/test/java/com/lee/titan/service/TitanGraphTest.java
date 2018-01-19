package com.lee.titan.service;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

public class TitanGraphTest {
	
	StandardTitanGraph graph;
	
	@Before
	public void init() {
		graph = (StandardTitanGraph) TitanFactory.open("conf/ent-graph.properties");		
	}
	
	@Test
	public void gtTest() {
		GraphTraversalSource g = graph.traversal();
		
		GraphTraversal<Vertex, Vertex> vs = g.V().has("name", "杭州飞梦科技有限公司").both().both().simplePath().has("name", "苏州惠商电子科技有限公司");
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void vertexIdTest() {
		TitanTransaction tx = graph.newTransaction();
		
		System.out.println("hello world");
		
		tx.commit();
	}
	
	@After
	public void clear() {
		graph.close();
	}
}
