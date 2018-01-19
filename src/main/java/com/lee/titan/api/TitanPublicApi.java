package com.lee.titan.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.lee.titan.domain.TitanPath;
import com.lee.titan.service.impl.TitanPublicServiceImpl;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.lee.titan.service.TitanPublicService;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

import spark.Spark;

public class TitanPublicApi {
	
	static TitanPublicService tp;
	static Gson gson;
	
	public static void main(String[] args) {
		// init.
		TitanGraph graph = TitanFactory.open("conf/ent-graph.properties");
		tp = new TitanPublicServiceImpl(graph);
		gson = new Gson();
		
		// listen port
		Spark.port(8039);
		
		//
		Spark. get("/titan/api/hello", (request, response) -> {
			response.status(200);
			response.type("text/plain; charset=utf-8"); 
			return "hello world";
        });
		
		// 1． 根据行业筛选公司
		Spark.get("/titan/api/tag/name/:tag/enterprise", (request, response) -> {
			String tag = request.params(":tag");
			List<Vertex> list = tp.vertices("tag", "name", tag, "v_type", 0);
			long vid = -1;
			if (list.size() > 0) {
				List<Map<String, Object>> rsList = new ArrayList<>();
				vid = list.get(0).value("v_id");
				List<Vertex> tags = tp.recursiveChild(vid);
				if (tags.size() > 0) {
					Map<String, Object> rs = null;
					for (Vertex vt : tags) {
						GraphTraversal<Vertex, Vertex> vs = tp.g().V(vt.id()).inE("企业标签").outV();
						while (vs.hasNext()) {
							Vertex v = vs.next();
							
							rs = new HashMap<>();
							rs.put("id", v.id());
							rs.put("label", v.label());
							rs.put("name", v.value("name"));
							rs.put("abstracts", v.value("abstracts"));
							rs.put("vid", v.value("v_id"));
							rs.put("vtype", v.value("v_type"));
							
							//
							rsList.add(rs);
						}
					}
				}
				response.status(200);
				response.type("application/json; charset=utf-8"); 
				return gson.toJson(rsList);
			} else {
				response.status(404);
				return "NOT FOUND";
			}
		});
		
		Spark.get("/titan/api/tag/vid/:vid/enterprise", (request, response) -> {
			long vid = Long.parseLong(request.params(":vid"));
			List<Vertex> tags = tp.recursiveChild(vid);
			if (tags.size() > 0) {
				List<Map<String, Object>> rsList = new ArrayList<>();
				Map<String, Object> rs = null;
				for (Vertex vt : tags) {
					GraphTraversal<Vertex, Vertex> vs = tp.g().V(vt.id()).inE("企业标签").outV();
					while (vs.hasNext()) {
						Vertex v = vs.next();
						
						rs = new HashMap<>();
						rs.put("id", v.id());
						rs.put("label", v.label());
						rs.put("name", v.value("name"));
						rs.put("abstracts", v.value("abstracts"));
						rs.put("vid", v.value("v_id"));
						rs.put("vtype", v.value("v_type"));
						
						//
						rsList.add(rs);
					}
				}
				response.status(200);
				response.type("application/json; charset=utf-8"); 
				return gson.toJson(rsList);
			} else {
				response.status(404);
				return "404 NOT FOUND";
			}
			
		});
		
		// 2． 根据轮次筛选公司
		Spark.get("/titan/api/round/:round/enterprise", (request, response) -> {
			String round = request.params(":round");
			List<Map<String, Object>> rsList = new ArrayList<>();
			Map<String, Object> rs = null;
			GraphTraversal<Edge, Vertex> vs = tp.g().E().has("e_financed_round", round).inV();
			while (vs.hasNext()) {
				Vertex v = vs.next();
				
				rs = new HashMap<>();
				rs.put("id", v.id());
				rs.put("label", v.label());
				rs.put("name", v.value("name"));
				rs.put("abstracts", v.value("abstracts"));
				rs.put("vid", v.value("v_id"));
				rs.put("vtype", v.value("v_type"));
				
				//
				rsList.add(rs);
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
		});
		
		// 3． 根据名称查公司基本属性
		Spark.get("/titan/api/enterprise/:name", (request, response) -> {
			String name = request.params(":name");
			
			List<Vertex> list = tp.vertices("enterprise", "name", name, "v_type", 1);
			if (list.size() > 0) {
				//
				List<Map<String, Object>> rsList = new ArrayList<>();
				Map<String, Object> rs = null;
				//
				String props = request.queryParams("props");// 属性列表
				if (null != props && props.length() > 0) {
					List<String> properties = gson.fromJson(props, new TypeToken<List<String>>() {
						private static final long serialVersionUID = -7661050766709266457L;
					}.getType());
					
					for (Vertex v : list) {
						rs = new HashMap<>();
						//
						for (String prop : properties) {
							rs.put(prop, v.value(prop));
						}
						//
						rsList.add(rs);
					}
				} else {
					for (Vertex v : list) {
						rs = new HashMap<>();
						//
						rs.put("id", v.id());
						rs.put("label", v.label());
						rs.put("name", name);
						rs.put("abstracts", v.value("abstracts"));
						rs.put("vid", v.value("v_id"));
						rs.put("vtype", v.value("v_type"));
						rs.put("corporator", v.value("ent_corporator"));
						
						//
						rsList.add(rs);
					}
				}
				
				response.status(200);
				response.type("application/json; charset=utf-8"); 
				return gson.toJson(rsList);
			} else {
				response.status(404);
				return "NOT FOUND";
			}
        });
		
		// 4． 根据产品名称查所属公司
		Spark.get("/titan/api/product/:name/enterprise", (request, response) -> {
			String name = request.params(":name");
			
			List<Map<String, Object>> rsList = new ArrayList<>();
			Map<String, Object> rs = null;
			GraphTraversal<Vertex, Vertex> vs = tp.g().V().has("name", name).has("v_type", 1).hasLabel("product").out("开发商");
			while (vs.hasNext()) {
				Vertex v = vs.next();
				
				rs = new HashMap<>();
				rs.put("id", v.id());
				rs.put("label", v.label());
				rs.put("name", v.value("name"));
				rs.put("abstracts", v.value("abstracts"));
				rs.put("vid", v.value("v_id"));
				rs.put("vtype", v.value("v_type"));
				
				//
				rsList.add(rs);
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
        });
		
		// 5． 查询公司的融资轮次
		Spark.get("/titan/api/enterprise/:name/round", (request, response) -> {
			String name = request.params(":name");
			
			List<Map<String, Object>> rsList = new ArrayList<>();
			Map<String, Object> rs = null;
			GraphTraversal<Vertex, Edge> vs = tp.g().V().has("name", name).has("v_type", 1).hasLabel("enterprise").inE("投资企业");
			while (vs.hasNext()) {
				Edge e = vs.next();
				
				rs = new HashMap<>();
				rs.put("id", e.id());
				rs.put("label", e.label());
				rs.put("e_financed_round", e.value("e_financed_round"));
				rs.put("e_financed_date", e.value("e_financed_date"));
				
				//
				rsList.add(rs);
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
        });
		
		// 6． 查询公司的基本属性（工商信息等）
		Spark.get("/titan/api/enterprise/:name/props", (request, response) -> {
			String name = request.params(":name");
			
			List<Vertex> list = tp.vertices("enterprise", "name", name, "v_type", 1);
			
			Map<String, Object> rs = new HashMap<>();
			if (list.size() > 0) {
				String props = request.queryParams("props");// 属性列表
				if (null != props && props.length() > 0) {
					List<String> properties = gson.fromJson(props, new TypeToken<List<String>>() {
						private static final long serialVersionUID = -7661050766709266457L;
					}.getType());
					
					Vertex v = list.get(0);
					for (String prop : properties) {
						rs.put(prop, v.value(prop));
					}
				} 
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rs);
		});
		
		// 7． 查询公司省份地区
		Spark.get("/titan/api/enterprise/:name/location", (request, response) -> {
			String name = request.params(":name");
			List<Vertex> list = tp.vertices("enterprise", "name", name, "v_type", 1);
			
			Map<String, Object> rs = new HashMap<>();
			if (list.size() > 0) {
				
				Vertex v = list.get(0);
				
				rs.put("province", v.value("ent_province"));
				rs.put("city", v.value("ent_city"));
				rs.put("location", v.value("ent_location"));
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rs);
		});
		
		// 8． 查询某投资机构投资的公司
		Spark.get("/titan/api/investment/:name/enterprise", (request, response) -> {
			String name = request.params(":name");
			
			List<Map<String, Object>> rsList = new ArrayList<>();
			Map<String, Object> rs = null;
			GraphTraversal<Vertex, Vertex> vs = tp.g().V().has("name", name).has("v_type", 1).hasLabel("investment").out("投资企业");
			while (vs.hasNext()) {
				Vertex v = vs.next();
				
				rs = new HashMap<>();
				rs.put("id", v.id());
				rs.put("label", v.label());
				rs.put("name", v.value("name"));
				rs.put("abstracts", v.value("abstracts"));
				rs.put("vid", v.value("v_id"));
				rs.put("vtype", v.value("v_type"));
				
				//
				rsList.add(rs);
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
        });
		
		// 9． 两个顶点间的路径
		Spark.get("/titan/api/:name1/path/:name2", (request, response) -> {
			String name1 = request.params(":name1");
			String name2 = request.params(":name2");
			
			String elable = request.queryParams("elabels");// 边label
			String vlable = request.queryParams("vlabels");// 顶点label
			int times = Integer.parseInt(null == request.queryParams("times") ? "5" : request.queryParams("times"));// 次数
			
			//
			GraphTraversal<Object, Vertex> sub;
			if (null != elable && elable.length() > 0) {
				List<String> list = gson.fromJson(elable, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				sub = __.bothE(list.toArray(new String[list.size()])).otherV();
			} else {
				sub = __.bothE().otherV();
			}
			
			if (null != vlable && vlable.length() > 0) {
				List<String> list = gson.fromJson(vlable, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				sub.hasLabel(list.toArray(new String[list.size()]));
			} 
			
			//
			List<List<Object>> rsList = new ArrayList<>();
			GraphTraversal<Vertex, Path> paths = tp.g().V().has("name", name1).has("v_type", 1)
					.repeat(sub.simplePath())
					.times(times)
					.has("name", name2).path().by("name").by(T.label);
			while (paths.hasNext()) {
				//
				rsList.add(paths.next().objects());
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
        });
		
		// 10.初创图谱
		Spark.get("/titan/api/:name1/stage/v1", (request, response) -> {
			String name1 = request.params(":name1");
			
			String elable = request.queryParams("elabels");// 边label
			String vlable = request.queryParams("vlabels");// 顶点label
			int times = Integer.parseInt(null == request.queryParams("times") ? "5" : request.queryParams("times"));// 次数
			
			//
			GraphTraversal<Object, Vertex> sub;
			if (null != elable && elable.length() > 0) {
				List<String> list = gson.fromJson(elable, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				sub = __.bothE(list.toArray(new String[list.size()])).otherV();
			} else {
				sub = __.bothE().otherV();
			}
			
			if (null != vlable && vlable.length() > 0) {
				List<String> list = gson.fromJson(vlable, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				sub.hasLabel(list.toArray(new String[list.size()]));
			} 
			
			//
			List<List<Object>> rsList = new ArrayList<>();
			GraphTraversal<Vertex, Path> paths = tp.g().V().has("name", name1).has("v_type", 1)
					.repeat(sub.simplePath())
					.times(times).path().by("name").by(T.label);
			while (paths.hasNext()) {
				//
				rsList.add(paths.next().objects());
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
        });
		
		// 有重复
		Spark.get("/titan/api/:name1/stage/v2", (request, response) -> {
			String name1 = request.params(":name1");
			
			String elabel = request.queryParams("elabels");// 边label
			String vlabel = request.queryParams("vlabels");// 顶点label
			int times = Integer.parseInt(null == request.queryParams("times") ? "5" : request.queryParams("times"));// 次数
			String edgeLabel = "投资企业";
			
			//
			GraphTraversal<Object, Vertex> sub;
			if (null != elabel && elabel.length() > 0) {
				List<String> list = gson.fromJson(elabel, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				sub = __.bothE(list.toArray(new String[list.size()])).otherV();
			} else {
				sub = __.bothE().otherV();
			}
			
			if (null != vlabel && vlabel.length() > 0) {
				List<String> list = gson.fromJson(vlabel, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				sub.hasLabel(list.toArray(new String[list.size()]));
			} 
			
			//
			List<Map<String, Object>> rsList = new ArrayList<>();
			GraphTraversal<Vertex, Path> paths = tp.g().V().has("name", name1).has("v_type", 1)
					.repeat(sub.simplePath())
					.times(times).path().by();
			while (paths.hasNext()) {
				//
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
					
					if (edgeLabel.equals(e.label())) {
						es.put("e_financed_round", e.value("e_financed_round"));
						es.put("e_financed_amount", e.value("e_financed_amount"));
						es.put("e_financed_date", e.value("e_financed_date"));
					}
					
					rsList.add(es);
				}
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
        });
		
		// 去重后
		Spark.get("/titan/api/:name1/stage/v3", (request, response) -> {
			String name1 = request.params(":name1");
			
			String elabel = request.queryParams("elabels");// 边label
			String vlabel = request.queryParams("vlabels");// 顶点label
			int times = Integer.parseInt(null == request.queryParams("times") ? "5" : request.queryParams("times"));// 次数
			String edgeLabel = "投资企业";
			
			//
			GraphTraversal<Object, Vertex> sub;
			if (null != elabel && elabel.length() > 0) {
				List<String> list = gson.fromJson(elabel, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				sub = __.bothE(list.toArray(new String[list.size()])).otherV();
			} else {
				sub = __.bothE().otherV();
			}
			
			if (null != vlabel && vlabel.length() > 0) {
				List<String> list = gson.fromJson(vlabel, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				sub.hasLabel(list.toArray(new String[list.size()]));
			} 
			
			//
			Set<TitanPath> rsList = new HashSet<>();
			GraphTraversal<Vertex, Path> paths = tp.g().V().has("name", name1).has("v_type", 1)
					.repeat(sub.simplePath())
					.times(times).path().by();
			while (paths.hasNext()) {
				//
				Path path = paths.next();
				Vertex out;
				Edge e;
				Vertex in;
				Map<String, Object> eproperties;
				for (int i = 0, len = path.size() - 2; i < len; i = i + 2) {
					e = path.get(i + 1);
					out = e.outVertex();
					in = e.inVertex();
					eproperties = new HashMap<>();
					
					if (edgeLabel.equals(e.label())) {
						eproperties.put("e_financed_round", e.value("e_financed_round"));
						eproperties.put("e_financed_amount", e.value("e_financed_amount"));
						eproperties.put("e_financed_date", e.value("e_financed_date"));
					}
					
					
					rsList.add(new TitanPath((Long) out.id(), 
							out.value("name"), 
							out.label(), 
							(Long) in.id(), 
							in.value("name"), 
							in.label(),
							e.label(), 
							eproperties));
				}
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
		});
		
		// 11.多个顶点的之间的路径
		Spark.get("/titan/api/vertices/paths/v1", (request, response) -> {
			String names = request.queryParams("names");// 边label
			int times = Integer.parseInt(null == request.queryParams("times") ? "4" : request.queryParams("times"));// 次数
			
			String elable = request.queryParams("elabels");// 边label
			String vlable = request.queryParams("vlabels");// 顶点label
			
			// repeat
			GraphTraversal<Object, Vertex> repeatTravers;
			if (null != elable && elable.length() > 0) {
				List<String> list = gson.fromJson(elable, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				repeatTravers = __.bothE(list.toArray(new String[list.size()])).otherV();
			} else {
				repeatTravers = __.bothE().otherV();
			}
			
			if (null != vlable && vlable.length() > 0) {
				List<String> list = gson.fromJson(vlable, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				repeatTravers.hasLabel(list.toArray(new String[list.size()]));
			} 
			
			//
			List<List<Object>> rsList = new ArrayList<>();
			if (null != names && names.length() > 0) {
				List<String> list = gson.fromJson(names, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				for (int i = 0; i < list.size(); i++) {
					String name = list.get(i);
					
					for (int j = i + 1; j < list.size(); j++) {
						GraphTraversal<Vertex, Path> paths = tp.g().V().has("name", name)
								.repeat(repeatTravers.asAdmin().clone().simplePath())
								.times(times).has("name", list.get(j)).path().by("name").by(T.label);
						while (paths.hasNext()) rsList.add(paths.next().objects());
					}
				}
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
        });
		
		// 11.多个顶点的之间的路径
		Spark.get("/titan/api/vertices/paths/v2", (request, response) -> {
			String names = request.queryParams("names");// 边label
			int times = Integer.parseInt(null == request.queryParams("times") ? "4" : request.queryParams("times"));// 次数
			
			String elable = request.queryParams("elabels");// 边label
			String vlable = request.queryParams("vlabels");// 顶点label
			
			// repeat
			GraphTraversal<Object, Vertex> repeatTravers;
			if (null != elable && elable.length() > 0) {
				List<String> list = gson.fromJson(elable, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				repeatTravers = __.bothE(list.toArray(new String[list.size()])).otherV();
			} else {
				repeatTravers = __.bothE().otherV();
			}
			
			if (null != vlable && vlable.length() > 0) {
				List<String> list = gson.fromJson(vlable, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				repeatTravers.hasLabel(list.toArray(new String[list.size()]));
			} 
			
			//
			List<List<Object>> rsList = new ArrayList<>();
			if (null != names && names.length() > 0) {
				List<String> list = gson.fromJson(names, new TypeToken<List<String>>() {
					private static final long serialVersionUID = -7661050766709266457L;
				}.getType());
				
				List<Object> vertexIds = new ArrayList<>();
				for (String name : list) {
					GraphTraversal<Vertex, Vertex> vs = tp.g().V().has("name", name).has("v_type", 1);
					while (vs.hasNext()) vertexIds.add(vs.next().id());
				}
				
				Iterator<Object> it = vertexIds.iterator();
				while (it.hasNext()) {
					Object vertexId = it.next();
					it.remove();
					
					if (!it.hasNext()) break;
					
					GraphTraversal<Vertex, Path> paths = tp.g().V(vertexId)
							.repeat(repeatTravers.asAdmin().clone().simplePath())
							.times(times).hasId(vertexIds.toArray()).path().by("name").by(T.label);
					while (paths.hasNext()) rsList.add(paths.next().objects());
				}
			}
			
			response.status(200);
			response.type("application/json; charset=utf-8"); 
			return gson.toJson(rsList);
			
        });
		
		
		// exception
		Spark.get("/throwexception", (request, response) -> {
		    throw new RuntimeException();
		});
		Spark.exception(RuntimeException.class, (exception, request, response) -> {
		    // Handle the exception here
			response.status(404);
		});
		
		
		// CROS
		Spark.after((request, response) -> {
		    response.header("Access-Control-Allow-Origin", "*");
		});
		
	}
}
