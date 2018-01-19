package com.lee.titan.app;

import com.lee.titan.service.TitanEditService;
import com.lee.titan.service.impl.TitanEditServiceImpl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class TitanApp {
	
	public static void main(String[] args) {
		
		TitanGraph graph = TitanFactory.open("conf/ent-graph.properties");
		
		TitanEditService editService = new TitanEditServiceImpl(graph);
		
		TitanManagement mgmt = editService.titanManagement();
//		editService.addVertexLabel(mgmt, "默认");// 缺省
//		editService.addVertexLabel(mgmt, "知识图谱");// 知识图谱
//		editService.addVertexLabel(mgmt, "人物");// 人物
//		editService.addVertexLabel(mgmt, "招聘职位");// 招聘职位
//		editService.addVertexLabel(mgmt, "产品");// 产品
//		editService.addVertexLabel(mgmt, "机构");// 机构
//		editService.addVertexLabel(mgmt, "标签");// 标签
//		editService.addVertexLabel(mgmt, "事件");// 事件
//		editService.addVertexLabel(mgmt, "省份");// 省份
//		editService.addVertexLabel(mgmt, "地区");// 地区
//		
//		editService.removeVertexLabel(mgmt, "kg");
//		editService.removeVertexLabel(mgmt, "person");
//		editService.removeVertexLabel(mgmt, "job");
//		editService.removeVertexLabel(mgmt, "product");
//		editService.removeVertexLabel(mgmt, "organization");
//		editService.removeVertexLabel(mgmt, "tag");
//		editService.removeVertexLabel(mgmt, "event");
//		editService.removeVertexLabel(mgmt, "province");
//		editService.removeVertexLabel(mgmt, "city");
		
//		editService.addVertexLabel(mgmt, "kg");// 知识图谱
//		editService.addVertexLabel(mgmt, "person");// 人物
//		editService.addVertexLabel(mgmt, "job");// 招聘职位
//		editService.addVertexLabel(mgmt, "product");// 产品
//		editService.addVertexLabel(mgmt, "organization");// 机构
//		editService.addVertexLabel(mgmt, "tag");// 标签
//		editService.addVertexLabel(mgmt, "event");// 事件
//		editService.addVertexLabel(mgmt, "province");// 省份
//		editService.addVertexLabel(mgmt, "city");// 地区
		
		
//		editService.addEdgeLabel(mgmt, "subClass", null);
		
		
		// vertex property.
//		editService.addPropertyKey(mgmt, "v_id", FieldType.LONG);
//		editService.addPropertyKey(mgmt, "name", FieldType.STRING);
//		editService.addPropertyKey(mgmt, "v_type", FieldType.INT);
//		editService.removePropertyKey(mgmt, "v_id");
//		editService.removePropertyKey(mgmt, "name");
//		editService.removePropertyKey(mgmt, "v_type");
		
		
		
//		editService.buildMixedIndex(mgmt, 
//				"search", 
//				"vertex", 
//				Vertex.class, 
//				null, 
//				"v_id", "int", 
//				"age", "int", 
//				"name", "string", 
//				"desc", "string");
//		mgmt.commit();
		
		
//		Vertex v0 = editService.addVertex("默认", "v_id", 0L, "name", "知识图谱", "v_type", 0);
//		Vertex v1 = editService.addVertex("知识图谱", "v_id", 1L, "name", "人物", "v_type", 0);
//		Vertex v2 = editService.addVertex("知识图谱", "v_id", 2L, "name", "招聘职位", "v_type", 0);
//		Vertex v3 = editService.addVertex("知识图谱", "v_id", 3L, "name", "产品", "v_type", 0);
//		Vertex v4 = editService.addVertex("知识图谱", "v_id", 4L, "name", "机构", "v_type", 0);
//		Vertex v5 = editService.addVertex("知识图谱", "v_id", 5L, "name", "标签", "v_type", 0);
//		Vertex v6 = editService.addVertex("知识图谱", "v_id", 6L, "name", "事件", "v_type", 0);
//		Vertex v7 = editService.addVertex("知识图谱", "v_id", 7L, "name", "省份", "v_type", 0);
//		Vertex v8 = editService.addVertex("知识图谱", "v_id", 8L, "name", "地区", "v_type", 0);
//		
//		editService.addEdge("subClass", v0, v1);
//		editService.addEdge("subClass", v0, v2);
//		editService.addEdge("subClass", v0, v3);
//		editService.addEdge("subClass", v0, v4);
//		editService.addEdge("subClass", v0, v5);
//		editService.addEdge("subClass", v0, v6);
//		editService.addEdge("subClass", v0, v7);
//		editService.addEdge("subClass", v0, v8);
		
		GraphTraversalSource g = graph.traversal();
		String name = (String) g.V().hasLabel("默认").next().property("name").value();
		System.out.println("kg name : " + name);
		System.out.println("-----------------------");
		GraphTraversal<Vertex, Vertex> vs = g.V().hasLabel(name);
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.id() + " " + v.label() + " " + v.property("v_id").value() + " " + v.property("name").value() + " " + v.property("v_type").value());
		}
		
		graph.close();
	}

}
