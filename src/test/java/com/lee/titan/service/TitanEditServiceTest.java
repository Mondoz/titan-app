package com.lee.titan.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import com.lee.titan.service.impl.TitanEditServiceImpl;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class TitanEditServiceTest {
	
	static TitanGraph graph;
	static TitanEditService editService;
	static TitanManagement mgmt;
	static GraphTraversalSource g;
	
	@BeforeClass
	public static void init() {
		graph = TitanFactory.open("conf/ent-graph.properties");
		editService = new TitanEditServiceImpl(graph);
		mgmt = editService.titanManagement();
		g = editService.graphTraversalSource();
	}
	
	@Test
	public void buildMixedIndexTest() {
		// 定义vertex和edge的property.
		PropertyKey v_id = mgmt.getPropertyKey("v_id");
		PropertyKey v_type = mgmt.getPropertyKey("v_type");
		PropertyKey v_name = mgmt.getPropertyKey("v_name");
		
		// 定义写入index的property.
		editService.buildMixedIndex(mgmt, "search", "vertices", 
						Vertex.class, null, v_id, v_type, v_name);
	}
	
	@Test
	public void addIndexKeyTest() {
		PropertyKey nick_name = editService.addPropertyKey(mgmt, "nick_name", "string", "single");
		TitanGraphIndex existsIndex = mgmt.getGraphIndex("vertices");
		editService.addIndexKey(mgmt, existsIndex, nick_name);
	}
	
	@Test
	public void listVertexLabelTest() {
		Iterable<VertexLabel> labels = mgmt.getVertexLabels();
		for (VertexLabel vl : labels) {
			System.out.println(vl.name() + " " + vl.property("name").value());
		}
	}
	
	@Test
	public void listLabelTest() {
		GraphTraversal<Vertex, Vertex> vs = graph.traversal().V().hasLabel("vertex");
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + v.property("v_id").value() + " " + v.property("name").value());
		}
	}
	
	@Test
	public void listPropertyTest() {
		PropertyKey v_id = graph.getPropertyKey("v_id");
		System.out.println(v_id);
	}
	
	@Test
	public void addPropertyKeyTest() {
		editService.addPropertyKey(mgmt, "name", "string", "single");
	}
	
	@Test
	public void getPropertyKeyTest() {
		PropertyKey pk = editService.getPropertyKey(mgmt, "name");
		System.out.println(pk.name());
	}
	
	@Ignore
	@Test
	public void addVertexTest() {
		Set<Long> vpid = new HashSet<>();
		vpid.add(0L);
		vpid.add(1L);
		editService.addVertex(null, 1L, 1, vpid, "彭治能");
	}
	
	@Test
	public void addVertexTest1() {
		Set<Long> vpid = new HashSet<>();
		vpid.add(0L);
		vpid.add(1L);
		vpid.add(2L);
		editService.addVertex(null, 1L, 1, vpid, "彭治能");
	}
	
	@Test
	public void removeVertexTest() {
		editService.removeVertex("", 1L, -1, 1, "彭治能");
	}
	
	@Test(expected=java.lang.NullPointerException.class)
	public void vertex() {
		Vertex v = editService.getVertex(null, 1L, -1, 1, "彭治能");
		System.out.println(v.label() + " " + v.id() + " " + 
				v.property("v_id").value() + " " + 
						v.property("v_type").value() + " " + 
				v.property("name").value());
	}
	
	@Test
	public void listVertex() {
		GraphTraversal<Vertex, Vertex> vs = graph.traversal().V().hasLabel("person");
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
							v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void vertexTest() {
		Vertex v = editService.vertex(0L);
		System.out.println(v.label() + " " + v.id() + " " + 
		v.property("v_id").value() + " " + 
				v.property("v_type").value() + " " + 
		v.property("name").value());
		Iterator<VertexProperty<Object>> v_p_id = v.properties("v_pids");
		while (v_p_id.hasNext()) {
			
			VertexProperty<Object> next = v_p_id.next();
			
			System.out.println(next.value());
		}
	}
	
	@Test
	public void vertex0Test() {
		
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_pids", 0L);
		
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
							v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void vertex1Test() {
		
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_id", 10L);
		
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
							v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void vertex2Test() {
		
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_id", 108123L);
		
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
							v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void vertex3Test() {
		
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("name", "北京小桔科技有限公司").has("v_type", 1L).hasLabel("enterprise");
		
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
							v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void vertex4Test() {
		
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V(49156140L);
		
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void addEdgeTest() {
		Vertex v = editService.vertex(0L);
		Vertex o = editService.vertex(1L);
		
		editService.addEdge("行业小类", v, o);
	}
	
	@Test
	public void addEdge2Test() {
		editService.addEdge("行业小类", 1L, 2L);
	}
	
	@Test
	public void removeEdgeTest() {
		editService.removeEdge("行业小类", 1L, 2L);
	}
	
	@Test
	public void listByEdgeLabel() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_id", 1L).outE("instance").inV();
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
							v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_id", 108130L).outE("现任机构").inV();
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
							v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest1() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_id", 10L).outE("股东").inV();
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest2() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_id", 193583L).outE("开发商").inV();
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest3() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("name", "橙品节操手机").hasLabel("product").outE("开发商").inV();
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest4() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_id", 253859L).outE("投资企业").inV();
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest5() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V().has("v_id", 5088L).inE("投资企业").outV();
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest6() {
		GraphTraversal<Vertex, Edge> es = editService.graphTraversalSource().V().has("v_id", 5088L).inE("投资企业");
		while (es.hasNext()) {
			Edge e = es.next();
			System.out.println(e.label() + " " + e.id() + " " + 
			e.property("e_financed_round").value() + " " + 
					e.property("e_tags"));
		}
	}
	
	@Test
	public void getByEdgeTest7() {
		GraphTraversal<Vertex, Edge> es = editService.graphTraversalSource().V().has("v_id", 5088L)
				.inE("投资企业").has("e_financed_round", "C轮");
		while (es.hasNext()) {
			Edge e = es.next();
			System.out.println(e.label() + " " + e.id() + " " + 
			e.property("e_financed_round").value() + " " + 
					e.value("e_tags"));
		}
	}
	
	@Test
	public void getByEdgeTest71() throws InterruptedException, ExecutionException {
		
		GraphTraversal<Edge, Vertex> es = editService.graphTraversalSource().E().has("e_financed_round", "IPO上市后").inV();
		while (es.hasNext()) {
			Vertex e = es.next();
			System.out.println(e.label() + " " + e.id() + " " + 
			e.property("name").value() + " " + 
					e.value("v_type"));
		}
		
	}
	
	@Test
	public void getByEdgeTest8() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V()
				.has("v_id", 5088L)
				.inE("投资企业")
				.has("e_financed_round", "C轮")
				.outV();
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest9() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V()
				.has("v_id", 263020L).outE("发布公司").inV();;
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest10() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V()
				.has("v_id", 13752L).inE("发布公司").outV();;
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest11() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V()
				.has("name", "黄岩").hasLabel("person").in("高管");// 武汉铃空网络科技有限公司 // 杭州飞梦科技有限公司
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest12() {
		GraphTraversal<Vertex, Vertex> vs = editService.graphTraversalSource().V()
				.has("name", "黄岩").out("现任机构");// 武汉铃空网络科技有限公司 // 杭州飞梦科技有限公司
		while (vs.hasNext()) {
			Vertex v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("v_id").value() + " " + 
					v.property("v_type").value() + " " + 
					v.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest13() {
		GraphTraversal<Vertex, Path> ps = editService.graphTraversalSource().V()
				.has("name", "北京小桔科技有限公司").repeat(__.both()).until(__.has("name", "阿里巴巴（中国）有限公司")).simplePath().path();
		while (ps.hasNext()) {
			Path p = ps.next();
			Vertex f = p.get(0);
			Vertex t = p.get(1);
			Vertex t3 = p.get(2);
			System.out.println(f.value("name") + " " + t.value("name") + " " + t3.value("name"));
		}
	}
	
	@Test
	public void getByEdgeTest14() {
		GraphTraversal<Vertex, Path> ps = g.V(2367704L).repeat(__.both("投资企业").simplePath()).until(__.has("name", "阿里巴巴（中国）有限公司")).path().by("name");
		while (ps.hasNext()) {
			Path p = ps.next();
			System.out.println(p);
		}
	}
	
	@Test
	public void getByEdgeTest15() {
		GraphTraversal<Vertex, Path> ps = g.V(2367704L).repeat(__.both("投资企业")).times(2).emit().has("name", "阿里巴巴（中国）有限公司").path().by("name");
		while (ps.hasNext()) {
			Path p = ps.next();
			System.out.println(p);
		}
	}
	
	@Test
	public void getByEdgeTest16() {
		GraphTraversal<Vertex, Path> ps = g.V(2367704L).repeat(__.bothE("投资企业").otherV().simplePath()).times(3).emit().has("name", "阿里巴巴（中国）有限公司").path().by("name");
		while (ps.hasNext()) {
			Path p = ps.next();
			System.out.println(p);
		}
	}
	
	@Test
	public void getByEdgeTest17() {
		GraphTraversal<Vertex, Path> ps = g.V(2367704L).repeat(__.bothE().has("name", "投资企业").otherV().simplePath()).times(2).emit().has("name", "阿里巴巴（中国）有限公司").path().by();
		while (ps.hasNext()) {
			Path p = ps.next();
			
			Vertex v1 = p.get(0);
			Edge e = p.get(1);
			Vertex v2 = p.get(2);
			
			System.out.println(v1.property("name").value());
			System.out.println((e.inVertex() == v1 ? "in" : "out") + e.property("e_financed_round").value() + e.property("e_financed_date").value());
			System.out.println(v2.property("name").value());
		}
	}
	
	@Test
	public void getByEdgeTest18() {
		GraphTraversal<Vertex, Path> ps = g.V(2367704L).repeat(__.bothE().has("name", "投资企业").otherV().simplePath()).times(2).emit().has("name", "阿里巴巴（中国）有限公司").path().by("name").by();
		while (ps.hasNext()) {
			Path p = ps.next();
			
			String v1 = p.get(0);
			Edge e = p.get(1);
			String v2 = p.get(2);
			
			
			System.out.println(v1);
			
			System.out.println(e.inVertex().property("abstracts").value());
			System.out.println(e.outVertex().property("abstracts").value());
			
			System.out.println(v2);
		}
	}
	
	@Test
	public void getByEdgeTest19() {
		GraphTraversal<Vertex, Path> ps = g.V(2367704L).repeat(__.bothE().hasLabel("投资企业").otherV().hasLabel("investment", "enterprise").simplePath()).times(3).has("name", "阿里巴巴（中国）有限公司").path().by("name").by(T.label);
		while (ps.hasNext()) {
			Path p = ps.next();
			System.out.println(p);
		}
	}
	
	@Test
	public void getByEdgeTest20() {
		
		long s = System.currentTimeMillis();
		
		List<String> names = new ArrayList<>();
		names.add("红杉资本");
		names.add("北京加双筷子科技有限公司");
		names.add("IDG资本");
		names.add("北京小桔科技有限公司");
		
		List<String> elabels = Arrays.asList("投资企业", "高管", "股东");
		List<String> vlabels = Arrays.asList("investment", "enterprise", "person");
		
		
		List<Path> rs = new ArrayList<>();
		for (int i = 0; i < names.size(); i++) {
			String name = names.get(i);
			
			for (int j = i + 1; j < names.size(); j++) {
				GraphTraversal<Vertex, Path> paths = g.V().has("name", name)
						.repeat(__.bothE(elabels.toArray(new String[0])).otherV().hasLabel(vlabels.toArray(new String[0])).simplePath())
						.times(3).emit().has("name", names.get(j)).path().by("name").by(T.label);
				while (paths.hasNext()) rs.add(paths.next());
			}
		}
		
		long e = System.currentTimeMillis();
		
		System.out.println(rs);
		System.out.println(e - s);
		
	}
	
	@Test
	public void getByEdgeTest21() {
		
		long s = System.currentTimeMillis();
		
		List<String> names = new ArrayList<>();
		names.add("红杉资本");
		names.add("北京加双筷子科技有限公司");
		names.add("IDG资本");
		names.add("北京小桔科技有限公司");
		names.add("金沙江创投");
		
		List<String> elabels = Arrays.asList("投资企业", "高管", "股东");
		List<String> vlabels = Arrays.asList("investment", "enterprise", "person");
		
		// repeat
		GraphTraversal<Object, Vertex> repeatTravers = __.bothE(elabels.toArray(new String[elabels.size()]))
				.otherV().hasLabel(vlabels.toArray(new String[vlabels.size()]));
		
		
		List<Path> rs = new ArrayList<>();
		
		List<Object> vertexIds = new ArrayList<>();
		for (String name : names) {
			GraphTraversal<Vertex, Vertex> vs = g.V().has("name", name).has("v_type", 1);
			while (vs.hasNext()) vertexIds.add(vs.next().id());
		}
		
		Iterator<Object> it = vertexIds.iterator();
		while (it.hasNext()) {
			Object vertexId = it.next();
			it.remove();
			
			if (!it.hasNext()) break;
			
			GraphTraversal<Vertex, Path> paths = g.V(vertexId)
					.repeat(repeatTravers.asAdmin().clone().simplePath())
					.times(3).hasId(vertexIds.toArray()).path().by("name").by(T.label);
			while (paths.hasNext()) rs.add(paths.next());
		}
		
		long e = System.currentTimeMillis();
		System.out.println(rs);
		System.out.println(e - s);
	}
	
	@Test
	public void getByEdgeTest22() {
		
		long s = System.currentTimeMillis();
		
		List<String> names = new ArrayList<>();
		names.add("红杉资本");
		names.add("北京加双筷子科技有限公司");
		names.add("IDG资本");
		names.add("北京小桔科技有限公司");
		
		List<String> elabels = Arrays.asList("投资企业", "高管", "股东");
		List<String> vlabels = Arrays.asList("investment", "enterprise", "person");
		
		// repeat
		GraphTraversal<Object, Vertex> repeatTravers = __.bothE(elabels.toArray(new String[elabels.size()]))
				.otherV().hasLabel(vlabels.toArray(new String[vlabels.size()]));
		
		
		List<Path> rs = new ArrayList<>();
		
		Iterator<String> it = names.iterator();
		while (it.hasNext()) {
			String name = it.next();
			it.remove();
			
			if (!it.hasNext()) break;
			
			GraphTraversal<Vertex, Path> paths = g.V().has("name", name)
					.repeat(repeatTravers.asAdmin().clone().simplePath())
					.times(3).has("name", P.within(names)).path().by("name").by(T.label);
			while (paths.hasNext()) rs.add(paths.next());
		}
		
		long e = System.currentTimeMillis();
		System.out.println(rs);
		System.out.println(e - s);
	}
	
	@Test
	public void getByEdgeTest23() {
		
		long s = System.currentTimeMillis();
		
		List<String> names = new ArrayList<>();
		names.add("红杉资本");
		names.add("北京加双筷子科技有限公司");
		names.add("IDG资本");
		names.add("北京小桔科技有限公司");
		
		List<String> elabels = Arrays.asList("投资企业", "高管", "股东");
		List<String> vlabels = Arrays.asList("investment", "enterprise", "person");
		
		// repeat
		GraphTraversal<Object, Vertex> repeatTravers = __.bothE(elabels.toArray(new String[elabels.size()]))
				.as("e")
				.otherV().hasLabel(vlabels.toArray(new String[vlabels.size()]));
		
		
		List<Path> rs = new ArrayList<>();
		
		List<Object> vertexIds = new ArrayList<>();
		for (String name : names) {
			GraphTraversal<Vertex, Vertex> vs = g.V().has("name", name).has("v_type", 1);
			while (vs.hasNext()) vertexIds.add(vs.next().id());
		}
		
		
		GraphTraversal<Vertex, Object> es = g.V(vertexIds.toArray())
				.as("first")
				.repeat(repeatTravers.asAdmin().clone().simplePath())
				.times(3).hasId(vertexIds.toArray())
				.as("last")
				.select("e")
				.order(Scope.local)
				.by(new Function<Element, Object>() {
					public Long apply(Element t) {
						return ((TitanEdge) t).longId();
					}
				}, Order.incr);
		while (es.hasNext()) System.out.println(es.next());		
		
		
		long e = System.currentTimeMillis();
		System.out.println(rs);
		System.out.println(e - s);
	}
	
	
	@Test
	public void getByEdgeTest24() {
		
		long s = System.currentTimeMillis();
		
		// [104738832, 1396760, 102879376, 2367704, 103538800]
		List<String> names = new ArrayList<>();
		names.add("红杉资本");
		names.add("北京加双筷子科技有限公司");
		names.add("IDG资本");
		names.add("北京小桔科技有限公司");
		names.add("金沙江创投");
		
		List<String> elabels = Arrays.asList("投资企业", "高管", "股东");
		List<String> vlabels = Arrays.asList("investment", "enterprise", "person");
		
		// repeat
		GraphTraversal<Object, Vertex> repeatTravers = __.bothE(elabels.toArray(new String[elabels.size()]))
				.as("e")
				.otherV().hasLabel(vlabels.toArray(new String[vlabels.size()]));
		
		
		List<Path> rs = new ArrayList<>();
		
		List<Object> vertexIds = new ArrayList<>();
		for (String name : names) {
			GraphTraversal<Vertex, Vertex> vs = g.V().has("name", name).has("v_type", 1);
			while (vs.hasNext()) vertexIds.add(vs.next().id());
		}
		
		/*
		g.V(1,6,5).repeat(bothE().as("e").otherV().simplePath()).times(3).hasId(1,6,5).dedup().by(select(all, "e").order(local).by(id)).
           path().by("name").by(label)
		 */
		GraphTraversal<Vertex, Path> paths = g.V(vertexIds.toArray())
				.repeat(repeatTravers.asAdmin().clone().simplePath())
				.times(3)
				.hasId(vertexIds.toArray())
				.dedup()
				.by(__.select(Pop.all, "e")
						.order(Scope.local)
						.by(new Function<Element, Object>() {
							public Long apply(Element t) {
								return ((TitanEdge) t).longId();
							}
						}, Order.incr))
				.path().by();
		
		
		while (paths.hasNext()) {
			 rs.add(paths.next());
		}
		
		long e = System.currentTimeMillis();
		System.out.println(rs);
		System.out.println(e - s);
	}
	
	@Test
	public void getByEdgeTest34() {
		
		long s = System.currentTimeMillis();
		
		// [104738832, 1396760, 102879376, 2367704, 103538800]
		List<String> names = new ArrayList<>();
		names.add("红杉资本");
		names.add("北京加双筷子科技有限公司");
//		names.add("IDG资本");
//		names.add("北京小桔科技有限公司");
//		names.add("金沙江创投");
		
		List<String> elabels = Arrays.asList("投资企业", "高管", "股东");
		List<String> vlabels = Arrays.asList("investment", "enterprise", "person");
		
		// repeat
		GraphTraversal<Object, Vertex> repeatTravers = __.bothE(elabels.toArray(new String[elabels.size()]))
				.as("e")
				.otherV().hasLabel(vlabels.toArray(new String[vlabels.size()]));
		
		
		List<Map<String, Object>> edges = new ArrayList<>();
		
		List<Object> vertexIds = new ArrayList<>();
		for (String name : names) {
			GraphTraversal<Vertex, Vertex> vs = g.V().has("name", name).has("v_type", 1);
			while (vs.hasNext()) vertexIds.add(vs.next().id());
		}
		
		GraphTraversal<Vertex, Path> paths = g.V(vertexIds.toArray())
				.repeat(repeatTravers.asAdmin().clone().simplePath())
				.times(3)
				.hasId(vertexIds.toArray())
				.dedup()
				.by(__.select(Pop.all, "e")
						.order(Scope.local)
						.by(new Function<Element, Object>() {
							public Long apply(Element t) {
								return ((TitanEdge) t).longId();
							}
						}, Order.incr))
				.path().by();
		
		
		while (paths.hasNext()) {
			Path path = paths.next();
			
			Vertex out;
			Edge e;
			Vertex in;
			Map<String, Object> es = null;
			for (int i = 0, len = path.size() - 2; i < len; i = i + 2) {
				e = path.get(i + 1);
				out = e.outVertex();
				in = e.inVertex();
				es = new HashMap<>();
				
				es.put("outId", out.id());
				es.put("outName", out.value("name"));
				es.put("outLabel", out.label());
				es.put("e_label", e.label());
				es.put("inId", in.id());
				es.put("inName", in.value("name"));
				es.put("inLabel", in.label());
				
				if ("投资企业".equals(e.label())) {
					es.put("e_financed_round", e.value("e_financed_round"));
					es.put("e_financed_amount", e.value("e_financed_amount"));
					es.put("e_financed_date", e.value("e_financed_round"));
				}
				
				edges.add(es);
			}
		}
		
		long e = System.currentTimeMillis();
		System.out.println(edges);
		System.out.println(e - s);
	}
	
	@Test
	public void getByEdgeTest25() {
		GraphTraversal<Vertex, Edge> vs = editService.graphTraversalSource().V()
				.has("name", "北京奥格睿码科技有限公司").has("v_type", 1).hasLabel("enterprise").inE("投资企业");//
		while (vs.hasNext()) {
			Edge v = vs.next();
			System.out.println(v.label() + " " + v.id() + " " + 
					v.property("e_financed_round").value());
		}
	}
	
	@Test
	public void getByEdgeTest26() {
		GraphTraversal<Vertex, Edge> vs = editService.graphTraversalSource().V()
				.has("name", "北京奥格睿码科技有限公司").has("v_type", 1).hasLabel("enterprise").inE("投资企业");//
		while (vs.hasNext()) {
			TitanEdge v = (TitanEdge) vs.next();
			
			System.out.println(v);
			
			// 182347790
			System.out.println(v.label() + " " + v.longId() + " " + 
					v.property("e_financed_round").value());
		}
	}
	
	@Test
	public void aggregateTest() {
		// 北京小桔科技有限公司
		GraphTraversal<Vertex, Edge> it = g.V().has("name", "北京小桔科技有限公司").inE("投资企业");
		while (it.hasNext()) {
			
			Edge e = it.next();
			Vertex v = e.outVertex();
			
			System.out.println(v.value("name") + " " + e.label() + " " + e.id() + " " + " " + e.value("e_financed_round"));
		}
	}
	
	
	@Test
	public void groupTest() {
		GraphTraversal<Vertex, Map<Object, Object>> vs = g.V().group().by(T.label).by(__.count());
		while (vs.hasNext()) System.out.println(vs.next());
	}
	
	@Test
	public void groupTest2() {
		GraphTraversal<Vertex, Vertex> enterprises = g.V().has("v_type", 1)
				.hasLabel("enterprise");
		
		while (enterprises.hasNext()) {
			Vertex ent = enterprises.next();
			long entId = (Long) ent.id();
			
			System.out.println(entId + " " + ent.value("name"));
			
			List<Object> tagList = g.V(entId).out("企业标签").values("name").toList();
			
			System.out.println(tagList);
		}
	}
	
	@Test
	public void tagTest() {
		List<Object> list = g.V().has("v_id", 17).out("企业标签").values("name").toList();
		System.out.println(list);
	}
	
	@Test
	public void valuesTest() {
		Iterator<Object> values = g.V().has("v_id", 255845L).next().values("v_pids", "v_id");
		System.out.println(Lists.newArrayList(values));
	}
	
	@Test
	public void valuesTest2() {
		List<Object> values = g.V().has("name", "彭治能").values("v_pids", "name").toList();
		System.out.println(values);// [彭治能, 0, 1, 2]
	}
	
	@Test
	public void valuesTest3() {
		Iterator<Object> values = g.V().has("name", "彭治能").next().values("v_pids", "name");
		System.out.println(Lists.newArrayList(values));// [彭治能, 0, 1, 2]
	}
	
	@Test
	public void valuesTest4() {
		List<Object> list = g.V(44376312)
				.has("name", "姜皓天")
				.as("a")
				.in("高管", "股东").values("name").toList();
		System.out.println(list);// 
	}
	
	@Test
	public void valuesTest5() {
		List<Edge> list = g.V(44376312)
				.has("name", "姜皓天")
				.inE("高管", "股东").toList();
		System.out.println(list);// 
	}
	
	@Test
	public void valuesTest6() {
		GraphTraversal<Vertex, Edge> es = g.V(104820928)
				.outE("投资企业").as("e").inV().hasId(2609248).select("e");
		if (es.hasNext()) {
			Edge e = es.next();
			
			System.out.println(Lists.newArrayList(e.values("e_tags")));
		}
	}
	
	@Test
	public void valuesTest7() {
		GraphTraversal<Vertex, Vertex> vs = g.V().has("name", "深圳市豆娱科技有限公司");
		if (vs.hasNext()) {
			Vertex v = vs.next();
			
			System.out.println(v.property("e_tags").isPresent());
			System.out.println(v.property("name").isPresent());
		}
	}
	
	@Test
	public void tt() {
		System.out.println(System.currentTimeMillis() -  14 * 60 * 60 * 1000);
	}
	
	@AfterClass
	public static void clear() {
		graph.close();
	}
}
