package com.lee.titan.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.lee.titan.service.TitanPublicService;
import com.thinkaurelius.titan.core.TitanGraph;

public class TitanPublicServiceImpl implements TitanPublicService {
	
	private final GraphTraversalSource g;
	
	public TitanPublicServiceImpl(TitanGraph graph) {
		this.g = graph.traversal();
	}
	
	public GraphTraversalSource g() {
		return g;
	}
	
	public Vertex id(long id) {
		Vertex rs = null;
		GraphTraversal<Vertex, Vertex> vs = g.V(id);
		if (vs.hasNext()) rs = vs.next();
		return rs;
	}

	public Vertex vid(long vid) {
		Vertex rs = null;
		GraphTraversal<Vertex, Vertex> vs = g.V().has("v_id", vid);
		if (vs.hasNext()) rs = vs.next();
		return rs;
	}

	public List<Vertex> vertices(String vertexLabel, Object... keyValues) {
		List<Vertex> rs = new ArrayList<Vertex>();
		if (null != keyValues && keyValues.length > 0) {
			GraphTraversal<Vertex, Vertex> vs = g.V();
			for (int i = 0; i < keyValues.length; i = i + 2) {
				vs.has(keyValues[i].toString(), keyValues[i + 1]);
			}
			
			//
			if (null != vertexLabel && vertexLabel.length() > 0) vs.hasLabel(vertexLabel);
			
			while (vs.hasNext()) {
				rs.add(vs.next());
			}
		}
		return rs;
	}

	public List<Vertex> out(long vid, int vtype, String name, String edgeLabel) {
		List<Vertex> rs = new ArrayList<Vertex>();
		GraphTraversal<Vertex, Vertex> vs = null;
		if (vid > -1) {
			vs = g.V().has("v_id", vid);
		} else if (null != name && name.length() > 0) {
			if (vtype > -1) {
				vs = g.V().has("name", name).has("v_type", vtype);
			} else {
				vs = g.V().has("name", name);
			}
		}
		
		//
		if (null != vs) {
			vs.out(edgeLabel);
			
			while (vs.hasNext()) {
				rs.add(vs.next());
			}
		}
		
		return rs;
	}
	
	//
	public List<Vertex> recursiveChild(long vid) {
		List<Vertex> rs = new ArrayList<Vertex>();
		// 子
		GraphTraversal<Vertex, Vertex> vs = g.V().has("v_pids", vid)
				.has("v_type", 0).hasLabel("tag");
		while (vs.hasNext()) {
			Vertex v = vs.next();
			rs.add(v);
			rs.addAll(recursiveChild(v.value("v_id")));
		}
		
		// 自己
		vs = g.V().has("v_id", vid)
				.has("v_type", 0).hasLabel("tag");
		
		if (vs.hasNext()) rs.add(vs.next());
		
		return rs;
	}
}
