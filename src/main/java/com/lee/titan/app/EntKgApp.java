package com.lee.titan.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.lee.titan.service.impl.TitanEditServiceImpl;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.lee.titan.service.TitanEditService;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class EntKgApp {
	
	public static void main(String[] args) throws UnsupportedEncodingException, IOException {
		// 17:10-18:15
		TitanGraph graph = TitanFactory.open("conf/ent-graph.properties");
		TitanEditService editService = new TitanEditServiceImpl(graph);
		EntKgApp app = new EntKgApp(editService);
		
		
		String entpath = "C:\\Users\\govert\\Desktop\\ent\\ent_es_20161102.txt";
		String perpath = "C:\\Users\\govert\\Desktop\\ent\\person_es_20161102.txt";
		String propath = "C:\\Users\\govert\\Desktop\\ent\\product_es_20161102.txt";
		String invepath = "C:\\Users\\govert\\Desktop\\ent\\ent_invest_es_20161102.txt";
		String tagpath = "C:\\Users\\govert\\Desktop\\ent\\tags_tree_prompt_es_20161102.txt";
		String finpath = "C:\\Users\\govert\\Desktop\\ent\\finance_es_20161102.txt";
		String jobpath = "C:\\Users\\govert\\Desktop\\ent\\job_es_20161102.txt";
		
		
		app.initSchema("conf/graph.schema", "utf-8");// 1L
		app.initConcept();// 
		
//		
		app.importEnterprise(entpath);// 17L-108140L
		app.importPerson(perpath);// 108140L-193583L
		app.importProduct(propath);// 193583L-252748L
		app.importInvestment(invepath);// 252748L-255845L
		app.importTag(tagpath);// 255845L-263016L
		app.importJob(jobpath);// 263016L-305114L
		
		app.exPersonEdge(perpath);//
		app.exProductEdge(propath);//
		app.exJobEdge(jobpath);
		app.exFinanceEdge(finpath);
		
		
		app.exEntTagEdge(entpath);
		app.exPersonTagEdge(perpath);
		app.exProductTagEdge(propath);
		app.exInvestmentTagEdge(invepath);
		
		graph.close();
	}
	
	
	public void initSchema(String pathname, String charsetName) throws UnsupportedEncodingException, IOException {
		// 
		TitanManagement mgmt = editService.titanManagement();
		
		String schema = new String(Files.readAllBytes(new File(pathname).toPath()), charsetName);
		String vertexLabel = schema.substring(schema.indexOf("vertexLabel") + 12, schema.indexOf("edgeLabel"));
		String edgeLabel = schema.substring(schema.indexOf("edgeLabel") + 10, schema.indexOf("property")).trim();
		String property = schema.substring(schema.indexOf("property") + 9).trim();
		
		JsonObject vlabel = gson.fromJson(vertexLabel, JsonObject.class);
		JsonObject elabel =  gson.fromJson(edgeLabel, JsonObject.class);
		JsonObject pk =  gson.fromJson(property, JsonObject.class);
		
		// 定义vertex的label.
		JsonArray vlabels = vlabel.getAsJsonArray("labels");
		for (JsonElement label : vlabels) {
			editService.addVertexLabel(mgmt, label.getAsString());
		}
		
		// 定义edge的label.
		JsonArray elables = elabel.getAsJsonArray("labels");
		for (JsonElement label : elables) {
			editService.addEdgeLabel(mgmt, label.getAsString(), null);
		}
		
		// 定义vertex和edge的property.
		editService.addPropertyKey(mgmt, "name", "string", "single");// 添加name属性
		JsonArray pks = pk.getAsJsonArray("pks");
		for (JsonElement label : pks) {
			JsonObject p = label.getAsJsonObject();
			String fname = p.get("name").getAsString();
			// name 已经添加过了
			if ("name".equals(fname)) continue;;
			
			String ftype = p.get("type").getAsString();
			String cardinality = p.get("cardinality") == null ? "single" : p.get("cardinality").getAsString();
			//
			editService.addPropertyKey(mgmt, fname, ftype, cardinality);
		}
		
		
		//
		// build Composite Index.
		// Composite indexes do not require configuration of an external indexing backend 
		// and are supported through the primary storage backend. 
		// 数据库层面的索引  这种索引查询的时候都是精确匹配并且索引的所有字段都要参与到查询中去
		mgmt.buildIndex("v_by_name", Vertex.class)
		.addKey(editService.getPropertyKey(mgmt, "name"))
		.buildCompositeIndex();
		mgmt.buildIndex("v_by_vid", Vertex.class)
		.addKey(editService.getPropertyKey(mgmt, "v_id"))
		.buildCompositeIndex();
		mgmt.buildIndex("v_by_name_vtype", Vertex.class)
		.addKey(editService.getPropertyKey(mgmt, "name"))
		.addKey(editService.getPropertyKey(mgmt, "v_type"))
		.buildCompositeIndex();
		
		//
		// build mixed index.
		// use elastic serach.
		//
		// vertex定义过的property建立索引
		mgmt.buildIndex("vertex", Vertex.class)
		.addKey(editService.getPropertyKey(mgmt, "name"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "abstracts"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "v_id"))
		.addKey(editService.getPropertyKey(mgmt, "v_type"))
		.addKey(editService.getPropertyKey(mgmt, "v_pids"))
		.addKey(editService.getPropertyKey(mgmt, "ent_status"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "ent_corporator"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "ent_type"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "ent_province"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "ent_city"), Mapping.STRING.asParameter())
		.buildMixedIndex("search");
		// edge定义过的property建立索引
		mgmt.buildIndex("edge", Edge.class)
		.addKey(editService.getPropertyKey(mgmt, "name"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "e_financed_year"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "e_financed_month"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "e_financed_round"), Mapping.STRING.asParameter())
		.addKey(editService.getPropertyKey(mgmt, "e_tags"), Mapping.STRING.asParameter())
		.buildMixedIndex("search");
		
		//
		// commit schema definition.
		//
		mgmt.commit();
		
		// 初始化顶级概念
		Set<Long> vpid = new HashSet<>();
		vpid.add(-1L);
		editService.addVertex(null, 0L, 0, vpid, "知识图谱");
		
		// commit the transaction to disk
		editService.titanGraph().tx().commit();
		LOGGER.info("titan shcema init done.");
	}
	
	public void initConcept() {
		Set<Long> vpid = new HashSet<>();
		vpid.add(0L);
		
		long vid = 1L;
		editService.addVertex("enterprise", vid++, 0, vpid, "公司");// 1L
		editService.addVertex("person", vid++, 0, vpid, "人物");// 2L
		editService.addVertex("product", vid++, 0, vpid, "产品");// 3L
		editService.addVertex("investment", vid++, 0, vpid, "投资机构");// 4L
		editService.addVertex("tag", vid++, 0, vpid, "标签");// 5L
		editService.addVertex("event", vid++, 0, vpid, "事件");// 6L
		editService.addVertex("job", vid++, 0, vpid, "招聘职位");// 7L
		
		// commit the transaction to disk
		editService.titanGraph().tx().commit();
		LOGGER.info("concept insert success, next vid " + vid);
	}
	
	public void importEnterprise(String pathname) throws FileNotFoundException, IOException {
		
		Set<Long> vpid = new HashSet<>();
		vpid.add(1L);
		
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 17L;// 108140L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject ent = gson.fromJson(line, JsonObject.class);
				String name = ent.get("name").getAsString();
				LOGGER.info("importing ent vertex... " + name);
				
				// 数值属性
				List<Object> kvs = new ArrayList<>();
				kvs.add("abstracts");
				kvs.add(ent.get("abstracts").getAsString());
				kvs.add("ent_register_capital");
				kvs.add(ent.get("regCap").getAsString());
				kvs.add("ent_location");
				kvs.add(ent.get("location").getAsString());
				kvs.add("ent_status");
				kvs.add(ent.get("enStatus").getAsString());
				kvs.add("ent_person");
				kvs.add(ent.get("entPersonNum").getAsString());
				kvs.add("ent_type");
				kvs.add(ent.get("entType").getAsString());
				kvs.add("ent_corporator");
				kvs.add(ent.get("corporator").getAsString());
				kvs.add("ent_establish_date");
				kvs.add(ent.get("esDate").getAsString());
				kvs.add("ent_province");
				kvs.add(ent.get("province").getAsString());
				kvs.add("ent_city");
				kvs.add(ent.get("city").getAsString());
				
				//
				editService.addVertex("enterprise", vid++, 1, vpid, name, kvs.toArray());
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("company insert success, next vid " + vid);
		}
	}
	
	public void importPerson(String pathname) throws FileNotFoundException, IOException {
		Set<Long> vpid = new HashSet<>();
		vpid.add(2L);
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 108140L;// 193583L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject person = gson.fromJson(line, JsonObject.class);
				String name = person.get("name").getAsString();
				LOGGER.info("importing person vertex... " + name);
				
				// 数值属性
				List<Object> kvs = new ArrayList<>();
				kvs.add("abstracts");
				kvs.add(person.get("abs").getAsString());
				//
				editService.addVertex("person", vid++, 1, vpid, name, kvs.toArray());
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("person insert success, next vid " + vid);
		}
		
	}
	
	public void importProduct(String pathname) throws FileNotFoundException, IOException {
		
		Set<Long> vpid = new HashSet<>();
		vpid.add(3L);
		
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 193583L;// 252748L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject product = gson.fromJson(line, JsonObject.class);
				String name = product.get("name").getAsString();
				LOGGER.info("importing product vertex... " + name);
				
				// 数值属性
				List<Object> kvs = new ArrayList<>();
				kvs.add("abstracts");
				kvs.add(product.get("abstracts").getAsString());
				
				//
				editService.addVertex("product", vid++, 1, vpid, name, kvs.toArray());
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("product insert success, next vid " + vid);
		}
	}
	
	public void importInvestment(String pathname) throws FileNotFoundException, IOException {
		
		Set<Long> vpid = new HashSet<>();
		vpid.add(4L);
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 252748L;// 255845L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject inves = gson.fromJson(line, JsonObject.class);
				String name = inves.get("name").getAsString();
				LOGGER.info("importing investment vertex... " + name);
				
				// 数值属性
				List<Object> kvs = new ArrayList<>();
				kvs.add("abstracts");
				kvs.add(inves.get("abstracts").getAsString());
				//
				editService.addVertex("investment", vid++, 1, vpid, name, kvs.toArray());
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("investment insert success, next vid " + vid);
		}
	}
	
	public void importTag(String pathname) throws FileNotFoundException, IOException {
		
		Set<Long> vpid = new HashSet<>();
		vpid.add(5L);
		
		
		long vid = 255845L;// 263016L
		editService.addVertex("tag", vid++, 0, vpid, "行业标签");// 255845L
		editService.addVertex("tag", vid++, 0, vpid, "背景标签");// 255846L
		editService.addVertex("tag", vid++, 0, vpid, "其他标签");// 255847L
		editService.addVertex("tag", vid++, 0, vpid, "专题");// 255848L
		
		Map<String, Long> tagNameVidMap = new HashMap<>();
		tagNameVidMap.put("行业标签", 255845L);
		tagNameVidMap.put("背景标签", 255846L);
		tagNameVidMap.put("其他标签", 255847L);
		tagNameVidMap.put("专题", 255848L);
		
		// 读取所有标签
		List<String> tags = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				tags.add(line);
			}
		}
		
		// 1602 中 父标签id和name映射
		Map<Integer, String> idNameMap = new HashMap<>();
		
		// 入子标签
		Set<Long> vpid16 = new HashSet<>();
		vpid16.add(255845L);
		Set<Long> vpid17 = new HashSet<>();
		vpid17.add(255846L);
		Set<Long> vpid18 = new HashSet<>();
		vpid18.add(255847L);
		Set<Long> vpid19 = new HashSet<>();
		vpid19.add(255848L);
		Iterator<String> firstTagsIterator = tags.iterator();
		while (firstTagsIterator.hasNext()) {
			//
			String line = firstTagsIterator.next();
			JsonObject tag = gson.fromJson(line, JsonObject.class);
			//
			int id = tag.get("id").getAsInt();
			String name = tag.get("name").getAsString();
			idNameMap.put(id, name);
			//
			int type = tag.get("type").getAsInt();
			if (type == 1601) {
				editService.addVertex("tag", vid++, 0, vpid16, name);
			} else if (type == 17) {
				editService.addVertex("tag", vid++, 0, vpid17, name);
			} else if (type == 18) {
				editService.addVertex("tag", vid++, 0, vpid18, name);
			} else if (type == 19) {
				editService.addVertex("tag", vid++, 0, vpid19, name);
			} else {
				continue;
			}
			
			LOGGER.info("importing tag vertex... " + name);
			
			tagNameVidMap.put(name, vid - 1);//
			firstTagsIterator.remove();//
		}
		
		// 二级标签
		Iterator<String> secondTagsIterator = tags.iterator();
		while (secondTagsIterator.hasNext()) {
			String line = secondTagsIterator.next();
			JsonObject tag = gson.fromJson(line, JsonObject.class);
			
			JsonArray pid = tag.getAsJsonArray("pid");
			Set<Long> vpids = new HashSet<>();
			for (JsonElement p : pid) {
				int pidp = p.getAsInt();
				String pname = idNameMap.get(pidp);
				vpids.add(tagNameVidMap.get(pname));
			}
			//
			String name = tag.get("name").getAsString();
			LOGGER.info("importing tag vertex... " + name);
			editService.addVertex("tag", vid++, 0, vpids, name);
			tagNameVidMap.put(name, vid - 1);//
		}
		
		// commit the transaction to disk
		editService.titanGraph().tx().commit();
		LOGGER.info("tag insert success, next vid " + vid);
	}
	
	public void importJob(String pathname) throws FileNotFoundException, IOException {
		
		Set<Long> vpid = new HashSet<>();
		vpid.add(7L);
		
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 263016L;// 305114L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject inves = gson.fromJson(line, JsonObject.class);
				String name = inves.get("name").getAsString();
				LOGGER.info("importing job vertex... " + name);
				
				// 数值属性
				List<Object> kvs = new ArrayList<>();
				kvs.add("job_salary");
				kvs.add(inves.get("salary").getAsString());
				kvs.add("job_publish_time");
				kvs.add(inves.get("publishTime").getAsString());
				kvs.add("job_edu_background");
				kvs.add(inves.get("education").getAsString());
				kvs.add("job_description");
				kvs.add(inves.get("positionDescription").getAsString());
				kvs.add("job_work_experience");
				kvs.add(inves.get("workExperience").getAsString());
				kvs.add("job_recruit_num");
				kvs.add(inves.get("recruitNum").getAsString());
				//
				editService.addVertex("job", vid++, 1, vpid, name, kvs.toArray());
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("job insert success, next vid " + vid);
		}
	}
	
	/*
	@Deprecated
	public void importFinanceEvent(String pathname) throws FileNotFoundException, IOException {
		
		Set<Long> vpid = new HashSet<>();
		vpid.add(6L);
		
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 305114L;// 319519L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject finances = gson.fromJson(line, JsonObject.class);
				LOGGER.info("importing finance event vertex.");
				
				// 融资轮次
				String round = null == finances.get("round") ? "" : finances.get("round").getAsString();
				// 融资年份
				String financed_year = null == finances.get("year") ? "" : finances.get("year").getAsString();
				// 融资月份
				String financed_month = null == finances.get("month") ? "" : finances.get("month").getAsString();
				// 融资日期
				String financed_date = null == finances.get("financedDate") ? "" : finances.get("financedDate").getAsString();
				// 融资额
				String financed_amount = null == finances.get("financed") ? "" : finances.get("financed").getAsString();
				//
				List<Object> keyValues = new ArrayList<>();
				keyValues.add("financed_round");
				keyValues.add(round);
				keyValues.add("financed_year");
				keyValues.add(financed_year);
				keyValues.add("financed_month");
				keyValues.add(financed_month);
				keyValues.add("financed_date");
				keyValues.add(financed_date);
				keyValues.add("financed_amount");
				keyValues.add(financed_amount);
				
				Vertex event = editService.addVertex("event", vid++, 1, vpid, "", keyValues.toArray());
				
				// 投资方
				JsonArray investPartyList = finances.getAsJsonArray("investPartyList");
				if (null != investPartyList && investPartyList.size() > 0) {
					// 投资方
					for (JsonElement invest : investPartyList) {
						JsonObject inve = (JsonObject) invest;
						String investName = inve.get("name").getAsString();
						Vertex inves = editService.getVertex("investment", -1, 4L, 1, investName);
						
						editService.addEdge("投资方", event, inves, keyValues.toArray());
					}
				}
				
				// 融资方
				JsonArray financedPartyList = finances.getAsJsonArray("financedPartyList");
				if (null != financedPartyList && financedPartyList.size() > 0) {
					// 融资方
					for (JsonElement entt : financedPartyList) {
						JsonObject en  = (JsonObject) entt;
						String companyName = en.get("name").getAsString();
						Vertex ent = editService.getVertex("enterprise", -1, 1L, 1, companyName);
						
						editService.addEdge("融资方", event, ent, keyValues.toArray());
					}
				}
				
				// 融资事件标签
				JsonArray tags = finances.getAsJsonArray("tags");
				if (null != tags && tags.size() > 0) {
					for (JsonElement t : tags) {
						String name = t.getAsString();
						
						Vertex tag = editService.getVertex("tag", -1, -1, 0, name);
						
						editService.addEdge("融资事件标签", event, tag);
					}
				}
				
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("finance event insert success, next vid " + vid);
		}
	}
	*/
	
	public void exPersonEdge(String pathname) throws FileNotFoundException, IOException {
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 108140L;// 193583L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject person = gson.fromJson(line, JsonObject.class);
				LOGGER.info("executing person edge... " + person.get("name").getAsString());
				
				JsonArray company = person.getAsJsonArray("company");
				if (null != company && company.size() > 0) {
					Vertex pe = editService.vertex(vid++);
					for (JsonElement c : company) {
						JsonObject co = (JsonObject) c;
						
						String companyName = co.get("companyName").getAsString();
						//
						
						Vertex ent = editService.getVertex("enterprise", -1, 1L, 1, companyName);
						// 人物到公司
						editService.addEdge("现任机构", pe, ent);
						String position = co.get("position").getAsString().trim();
						String[] ps = position.split(",");
						for (String pos : ps) {
							// 公司到人物
							editService.addEdge(pos, ent, pe);
						}
						
					}
				}
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("person edge insert success.");
		}
	}
	
	public void exProductEdge(String pathname) throws FileNotFoundException, IOException {
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 193583L;// 252748L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject product = gson.fromJson(line, JsonObject.class);
				LOGGER.info("executing product edge... " + product.get("name").getAsString());
				
				JsonObject company = product.getAsJsonObject("company");
				if (null != company && company.size() > 0) {
					String companyName = company.entrySet().iterator().next().getValue().getAsString();
					Vertex ent = editService.getVertex("enterprise", -1, 1L, 1, companyName);
					Vertex pdct = editService.vertex(vid++);
					editService.addEdge("开发商", pdct, ent);
				}
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("product edge insert success.");
		}
	}
	
	public void exFinanceEdge(String pathname) throws FileNotFoundException, IOException {
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject finances = gson.fromJson(line, JsonObject.class);
				LOGGER.info("executing finance edge...");
				
				JsonArray investPartyList = finances.getAsJsonArray("investPartyList");
				JsonArray financedPartyList = finances.getAsJsonArray("financedPartyList");
				if (null != investPartyList && investPartyList.size() > 0 && 
						null != financedPartyList && financedPartyList.size() > 0) {
					// 融资轮次
					String round = null == finances.get("round") ? "" : finances.get("round").getAsString();
					// 融资年份
					String e_financed_year = null == finances.get("year") ? "" : finances.get("year").getAsString();
					// 融资月份
					String e_financed_month = null == finances.get("month") ? "" : finances.get("month").getAsString();
					// 融资日期
					String e_financed_date = null == finances.get("financedDate") ? "" : finances.get("financedDate").getAsString();
					// 融资额
					String e_financed_amount = null == finances.get("financed") ? "" : finances.get("financed").getAsString();
					
					// 投资方
					for (JsonElement invest : investPartyList) {
						JsonObject inve = (JsonObject) invest;
						
						String investName = inve.get("name").getAsString();
						Vertex inves = editService.getVertex("investment", -1, 4L, 1, investName);
						// 融资方
						for (JsonElement entt : financedPartyList) {
							JsonObject en  = (JsonObject) entt;
							String companyName = en.get("name").getAsString();
							Vertex ent = editService.getVertex("enterprise", -1, 1L, 1, companyName);
							
							// 融资事件边上的属性
							List<Object> keyValues = new ArrayList<>();
							// keyValues.add("name");
							// keyValues.add("投资企业");
							keyValues.add("e_financed_round");
							keyValues.add(round);
							keyValues.add("e_financed_year");
							keyValues.add(e_financed_year);
							keyValues.add("e_financed_month");
							keyValues.add(e_financed_month);
							keyValues.add("e_financed_date");
							keyValues.add(e_financed_date);
							keyValues.add("e_financed_amount");
							keyValues.add(e_financed_amount);
							// 融资事件标签(也就是融资方的行业标签)
							JsonArray tags = finances.getAsJsonArray("tags");
							if (null != tags && tags.size() > 0) {
								StringBuilder e_tags = new StringBuilder();
								for (JsonElement t : tags) {
									e_tags.append(t.getAsString()).append(",");
								}
								keyValues.add("e_tags");
								keyValues.add(e_tags.deleteCharAt(e_tags.lastIndexOf(",")).toString());
							}
							
							editService.addEdge("投资企业", inves, ent, keyValues.toArray());
						}
						
					}
				}
				
			}
			
			// commit the transaction to disk.
			editService.titanGraph().tx().commit();
			LOGGER.info("finance edge insert success.");
		}
	}
	
	public void exJobEdge(String pathname) throws FileNotFoundException, IOException {
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 263016L;//
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject product = gson.fromJson(line, JsonObject.class);
				LOGGER.info("executing job edge... " + product.get("name").getAsString());
				
				JsonObject company = product.getAsJsonObject("publishCompany");
				if (null != company && company.size() > 0) {
					String companyName = company.entrySet().iterator().next().getValue().getAsString();
					Vertex job = editService.vertex(vid++);
					Vertex ent = editService.getVertex("enterprise", -1, 1L, 1, companyName);
					editService.addEdge("发布公司", job, ent);
				}
			}
			
			// commit the transaction to disk.
			editService.titanGraph().tx().commit();
			LOGGER.info("job edge insert success.");
		}
	}
	
	
	public void exEntTagEdge(String pathname) throws FileNotFoundException, IOException {
		
		// 企业标签
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 17L;// 108140L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject ent = gson.fromJson(line, JsonObject.class);
				LOGGER.info("executing ent tag edge... " + ent.get("name").getAsString());
				
				JsonArray tags = ent.getAsJsonArray("tags");
				if (null != tags && tags.size() > 0) {
					Vertex en = editService.vertex(vid++);
					for (JsonElement t : tags) {
						String name = t.getAsString();
						
						Vertex tag = editService.getVertex("tag", -1, -1, 0, name);
						
						editService.addEdge("企业标签", en, tag);
					}
				}
				
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("company tag edge insert success, next vid " + vid);
		}
	}
	
	public void exPersonTagEdge(String pathname) throws FileNotFoundException, IOException {
		
		// 人物标签
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 108140L;// 193583L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject person = gson.fromJson(line, JsonObject.class);
				LOGGER.info("executing person tag edge... " + person.get("name").getAsString());
				
				JsonArray tags = person.getAsJsonArray("tags");
				if (null != tags && tags.size() > 0) {
					Vertex pe = editService.vertex(vid++);
					for (JsonElement c : tags) {
						
						String tag = c.getAsString();
						//
						
						Vertex ta = editService.getVertex("tag", -1, -1, 0, tag);
						// 
						editService.addEdge("人物标签", pe, ta);
					}
				}
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("person tag edge insert success.");
		}
	}
	
	public void exProductTagEdge(String pathname) throws FileNotFoundException, IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 193583L;// 252748L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject product = gson.fromJson(line, JsonObject.class);
				LOGGER.info("executing product tag edge... " + product.get("name").getAsString());
				
				JsonArray tags = product.getAsJsonArray("tags");
				if (null != tags && tags.size() > 0) {
					Vertex prdt = editService.vertex(vid++);
					for (JsonElement c : tags) {
						
						String tag = c.getAsString();
						//
						Vertex ta = editService.getVertex("tag", -1, -1, 0, tag);
						// 
						editService.addEdge("产品标签", prdt, ta);
					}
				}
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("product tag edge insert success.");
		}
	}
	
	public void exInvestmentTagEdge(String pathname) throws FileNotFoundException, IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(new File(pathname)))) {
			long vid = 252748L;// 255845L
			while (true) {
				String line = br.readLine();
				if (null == line) break;;
				
				//
				line = line.trim();
				if (line.length() < 1) continue;;
				
				//
				JsonObject invest = gson.fromJson(line, JsonObject.class);
				LOGGER.info("executing investment tag edge... " + invest.get("name").getAsString());
				
				JsonArray tags = invest.getAsJsonArray("investIndustry");
				if (null != tags && tags.size() > 0) {
					Vertex prdt = editService.vertex(vid++);
					for (JsonElement c : tags) {
						
						String tag = c.getAsString();
						//
						Vertex ta = editService.getVertex("tag", -1, -1, 0, tag);
						// 
						editService.addEdge("投资机构标签", prdt, ta);
					}
				}
			}
			
			// commit the transaction to disk
			editService.titanGraph().tx().commit();
			LOGGER.info("investment tag edge insert success.");
		}
	}
	
	public EntKgApp(TitanEditService editService) {
		this.editService = editService;
		this.gson = new Gson();
	}
	
	//
	private final TitanEditService editService;
	private final Gson gson;
	
	private static final Logger LOGGER = Logger.getLogger(EntKgApp.class);
}
