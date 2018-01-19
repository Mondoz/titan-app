package com.lee.titan.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

/**
 * Created by govert on 2016/11/28.
 */
public class EntKgExport {
	
	static final Gson gson = new Gson();
	
    public static void main(String[] args) throws IOException {
    	
    	TitanGraph graph = TitanFactory.open("conf/ent-graph.properties");
		EntKgExport app = new EntKgExport(graph);
		
		
    	String entpath = "C:\\Users\\govert\\Desktop\\ent_ex\\ent_es_20161102.txt";
		String perpath = "C:\\Users\\govert\\Desktop\\ent_ex\\person_es_20161102.txt";
		String propath = "C:\\Users\\govert\\Desktop\\ent_ex\\product_es_20161102.txt";
		String invepath = "C:\\Users\\govert\\Desktop\\ent_ex\\ent_invest_es_20161102.txt";
		String tagpath = "C:\\Users\\govert\\Desktop\\ent_ex\\tags_es_20161102.txt";
		String finpath = "C:\\Users\\govert\\Desktop\\ent_ex\\finance_es_20161102.txt";
		String jobpath = "C:\\Users\\govert\\Desktop\\ent_ex\\job_es_20161102.txt";

		//
		app.exportEnterprise(entpath);
		app.exportTag(tagpath);
		app.exportPerson(perpath);
		app.exportProduct(propath);
		app.exportInvestment(invepath);
		app.exportJob(jobpath);
		app.exportFinance(finpath);
		
		// close graph.
		graph.close();
    }
    
    public void exportEnterprise(String pathname) throws IOException {
    	try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathname)))) {
    		
    		LOGGER.info("export enterprise vertex init.");
    		
    		GraphTraversal<Vertex, Vertex> enterprises = g.V().has("v_type", 1).hasLabel("enterprise");
    		Vertex v;
    		Map<String, Object> ent;
    		while (enterprises.hasNext()) {
    			v = enterprises.next();
    			ent = new HashMap<>();
    			
    			LOGGER.info("export enterprise vertex... " + v.value("name"));
    			
    			long id = (Long) v.id();
    			// 基本属性
    			ent.put("id", v.id());
    			ent.put("v_id", v.value("v_id"));
    			ent.put("v_pids", Lists.newArrayList(v.values("v_pids")));
    			ent.put("v_type", v.value("v_type"));
    			ent.put("name", v.value("name"));
    			ent.put("abstracts", v.value("abstracts"));
    			ent.put("ent_register_capital", v.value("ent_register_capital"));
    			ent.put("ent_location", v.value("ent_location"));
    			ent.put("ent_status", v.value("ent_status"));
    			ent.put("ent_person", v.value("ent_person"));
    			ent.put("ent_type", v.value("ent_type"));
    			ent.put("ent_corporator", v.value("ent_corporator"));
    			ent.put("ent_establish_date", v.value("ent_establish_date"));
    			ent.put("ent_province", v.value("ent_province"));
    			ent.put("ent_city", v.value("ent_city"));
    			
    			// 标签
    			ent.put("tags", g.V(id).out("企业标签").values("name").toList());
    			
    			bw.write(gson.toJson(ent));
    			bw.newLine();
    		}
    		
    		
    		LOGGER.info("export enterprise vertex done.");
    	}
    }
    
    public void exportPerson(String pathname) throws IOException {
    	try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathname)))) {
    		
    		LOGGER.info("export person vertex init.");
    		
    		GraphTraversal<Vertex, Vertex> enterprises = g.V().has("v_type", 1).hasLabel("person");
    		Vertex v;
    		Map<String, Object> person;
    		while (enterprises.hasNext()) {
    			v = enterprises.next();
    			person = new HashMap<>();
    			
    			LOGGER.info("export person vertex... " + v.value("name"));
    			
    			long id = (Long) v.id();
    			// 基本属性
    			person.put("id", v.id());
    			person.put("v_id", v.value("v_id"));
    			person.put("v_pids", Lists.newArrayList(v.values("v_pids")));
    			person.put("v_type", v.value("v_type"));
    			person.put("name", v.value("name"));
    			person.put("abstracts", v.value("abstracts"));
    			
    			// 公司
    			List<Map<String, Object>> companyList = new ArrayList<>();
    			List<Edge> ents = g.V(id).inE("高管", "股东").toList();
    			if (ents.size() > 0) {
    				Edge ev = ents.get(0);
    				Vertex tv = ev.outVertex();
    				Map<String, Object> company = new HashMap<>();
    				
    				company.put("id", tv.id());
    				company.put("v_id", tv.value("v_id"));
    				company.put("v_pids", tv.value("v_pids"));
    				company.put("v_type", tv.value("v_type"));
    				company.put("name", tv.value("name"));
    				company.put("position", ents.size() == 2 ? "高管,股东" : ev.label());
    				
    				companyList.add(company);
    			}
    			person.put("company", companyList);
    			
    			// 标签
    			person.put("tags", g.V(id).out("人物标签").values("name").toList());
    			
    			bw.write(gson.toJson(person));
    			bw.newLine();
    		}
    		
    		LOGGER.info("export person vertex done.");
    		
    	}
    }
    
    public void exportProduct(String pathname) throws IOException {
    	try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathname)))) {
    		
    		LOGGER.info("export product vertex init.");
    		
    		GraphTraversal<Vertex, Vertex> products = g.V().has("v_type", 1).hasLabel("product");
    		Vertex v;
    		Map<String, Object> product;
    		while (products.hasNext()) {
    			v = products.next();
    			product = new HashMap<>();
    			
    			LOGGER.info("export product vertex... " + v.value("name"));
    			
    			long id = (Long) v.id();
    			// 基本属性
    			product.put("id", v.id());
    			product.put("v_id", v.value("v_id"));
    			product.put("v_pids", Lists.newArrayList(v.values("v_pids")));
    			product.put("v_type", v.value("v_type"));
    			product.put("name", v.value("name"));
    			product.put("abstracts", v.value("abstracts"));
    			
    			// 公司
    			Map<String, Object> company = new HashMap<>();
    			GraphTraversal<Vertex, Vertex> coms = g.V(id).out("开发商");
    			if (coms.hasNext()) {
    				Vertex tv = coms.next();
    				
    				company.put("id", tv.id());
    				company.put("v_id", tv.value("v_id"));
    				company.put("v_pids", tv.value("v_pids"));
    				company.put("v_type", tv.value("v_type"));
    				company.put("name", tv.value("name"));
    			}
    			product.put("company", company);
    			
    			// 标签
    			product.put("tags", g.V(id).out("产品标签").values("name").toList());
    			
    			bw.write(gson.toJson(product));
    			bw.newLine();
    		}
    		
    		LOGGER.info("export product vertex done.");
    	}
    }
    
    public void exportInvestment(String pathname) throws IOException {
    	try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathname)))) {
    		
    		LOGGER.info("export investment vertex init.");
    		
    		GraphTraversal<Vertex, Vertex> investments = g.V().has("v_type", 1).hasLabel("investment");
    		Vertex v;
    		Map<String, Object> investment;
    		while (investments.hasNext()) {
    			v = investments.next();
    			investment = new HashMap<>();
    			
    			LOGGER.info("export investment vertex... " + v.value("name"));
    			
    			long id = (Long) v.id();
    			// 基本属性
    			investment.put("id", v.id());
    			investment.put("v_id", v.value("v_id"));
    			investment.put("v_pids", Lists.newArrayList(v.values("v_pids")));
    			investment.put("v_type", v.value("v_type"));
    			investment.put("name", v.value("name"));
    			investment.put("abstracts", v.value("abstracts"));
    			
    			// 投资数量
    			int investNum = g.V(id).out("投资企业").toList().size();
    			investment.put("investNum", investNum);
    			
    			// 标签
    			investment.put("tags", g.V(id).out("投资机构标签").values("name").toList());
    			
    			bw.write(gson.toJson(investment));
    			bw.newLine();
    		}
    		
    		LOGGER.info("export investment vertex done.");
    		
    	}
    }
    
    public void exportTag(String pathname) throws IOException {
    	try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathname)))) {
    		
    		LOGGER.info("export tag vertex init.");
    		
    		GraphTraversal<Vertex, Vertex> tags = g.V().has("v_type", 0).hasLabel("tag");
    		Vertex tag;
    		Map<String, Object> tmap;
    		while (tags.hasNext()) {
    			tag = tags.next();
    			tmap = new HashMap<>();
    			
    			LOGGER.info("export tag vertex... " + tag.value("name"));
    			
    			tmap.put("id", tag.id());
    			tmap.put("v_id", tag.value("v_id"));
    			tmap.put("v_pids", Lists.newArrayList(tag.values("v_pids")));
    			tmap.put("name", tag.value("name"));
    			
    			bw.write(gson.toJson(tmap));
    			bw.newLine();
    		}
    		
    		LOGGER.info("export tag vertex done.");
    		
    	}
    }
    
    public void exportJob(String pathname) throws IOException {
    	try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathname)))) {
    		
    		LOGGER.info("export job vertex init.");
    		
    		GraphTraversal<Vertex, Vertex> jobs = g.V().has("v_type", 1).hasLabel("job");
    		Vertex v;
    		Map<String, Object> job;
    		while (jobs.hasNext()) {
    			v = jobs.next();
    			job = new HashMap<>();
    			
    			LOGGER.info("export job vertex... " + v.value("name"));
    			
    			long id = (Long) v.id();
    			// 基本属性
    			job.put("id", v.id());
    			job.put("v_id", v.value("v_id"));
    			job.put("v_pids", Lists.newArrayList(v.values("v_pids")));
    			job.put("v_type", v.value("v_type"));
    			job.put("name", v.value("name"));
    			job.put("job_salary", v.value("job_salary"));
    			job.put("job_publish_time", v.value("job_publish_time"));
    			job.put("job_edu_background", v.value("job_edu_background"));
    			job.put("job_description", v.value("job_description"));
    			job.put("job_work_experience", v.value("job_work_experience"));
    			job.put("job_recruit_num", v.value("job_recruit_num"));
    			
    			// 公司
    			Map<String, Object> company = new HashMap<>();
    			GraphTraversal<Vertex, Vertex> coms = g.V(id).out("发布公司");
    			if (coms.hasNext()) {
    				Vertex tv = coms.next();
    				
    				company.put("id", tv.id());
    				company.put("v_id", tv.value("v_id"));
    				company.put("v_pids", tv.value("v_pids"));
    				company.put("v_type", tv.value("v_type"));
    				company.put("name", tv.value("name"));
    			}
    			job.put("publishCompany", company);
    			
    			bw.write(gson.toJson(job));
    			bw.newLine();
    		}
    		
    		LOGGER.info("export job vertex done.");
    		
    	}
    }
    
    /*
    @Deprecated
    public void exportFinanceEvent(String pathname) throws IOException {
    	
    	try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathname)))) {
    		
    		LOGGER.info("export finance event vertex init.");
    		
    		GraphTraversal<Vertex, Vertex> events = g.V().has("v_type", 1).hasLabel("event");
    		Vertex v;
    		Map<String, Object> event;
    		while (events.hasNext()) {
    			v = events.next();
    			event = new HashMap<>();
    			
    			LOGGER.info("export finance event vertex... " + v.value("name"));
    			
    			long id = (Long) v.id();
    			// 基本属性
    			event.put("id", v.id());
    			event.put("v_id", v.value("v_id"));
    			event.put("v_pids", Lists.newArrayList(v.values("v_pids")));
    			event.put("v_type", v.value("v_type"));
    			event.put("financed_round", v.value("financed_round"));
    			event.put("financed_year", v.value("financed_year"));
    			event.put("financed_month", v.value("financed_month"));
    			event.put("financed_date", v.value("financed_date"));
    			event.put("financed_amount", v.value("financed_amount"));
    			
    			// 投资方
    			List<Map<String, Object>> investList = new ArrayList<>();
    			GraphTraversal<Vertex, Vertex> invests = g.V(id).out("投资方");
    			Vertex inve;
    			Map<String, Object> invest;
    			while (invests.hasNext()) {
    				
    				inve = invests.next();
    				invest = new HashMap<>();
    				
    				//
    				invest.put("id", inve.id());
        			invest.put("v_id", inve.value("v_id"));
        			invest.put("v_pids", Lists.newArrayList(inve.values("v_pids")));
        			invest.put("v_type", inve.value("v_type"));
        			invest.put("name", inve.value("name"));
    				
    				investList.add(invest);
    			}
    			event.put("investPartyList", investList);
    			
    			
    			// 融资方
    			List<Map<String, Object>> entList = new ArrayList<>();
    			GraphTraversal<Vertex, Vertex> fins = g.V(id).out("融资方");
    			Vertex fin;
    			Map<String, Object> ent;
    			while (fins.hasNext()) {
    				
    				fin = fins.next();
    				ent = new HashMap<>();
    				
    				//
    				ent.put("id", fin.id());
        			ent.put("v_id", fin.value("v_id"));
        			ent.put("v_pids", Lists.newArrayList(fin.values("v_pids")));
        			ent.put("v_type", fin.value("v_type"));
        			ent.put("name", fin.value("name"));
        			
        			entList.add(ent);
    			}
    			event.put("financedPartyList", entList);
    			
    			// 标签
    			event.put("tags", g.V(id).out("融资事件标签").values("name").toList());
    			
    			//
    			bw.write(gson.toJson(event));
    			bw.newLine();
    		}
    		
    		LOGGER.info("export finance event vertex init.");
    		
    	}
    }
    */
    
    public void exportFinance(String pathname) throws IOException {
    	
    	try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathname)))) {
    		
    		LOGGER.info("export finance edge init.");
    		
    		GraphTraversal<Vertex, Edge> finances = g.V().has("v_type", 1).hasLabel("investment").outE("投资企业");
    		TitanEdge e;
    		Map<String, Object> finance;
    		while (finances.hasNext()) {
    			e = (TitanEdge) finances.next();
    			finance = new HashMap<>();
    			
    			LOGGER.info("export finance edge.");
    			
    			// 边上的属性
    			finance.put("id", e.longId());
    			finance.put("e_financed_round", e.value("e_financed_round"));
    			finance.put("e_financed_year", e.value("e_financed_year"));
    			finance.put("e_financed_month", e.value("e_financed_month"));
    			finance.put("e_financed_date", e.value("e_financed_date"));
    			finance.put("e_financed_amount", e.value("e_financed_amount"));
    			
    			// 投资方
    			List<Map<String, Object>> investList = new ArrayList<>();
    			Vertex out = e.outVertex();
    			Map<String, Object> invest = new HashMap<>();
    			invest.put("id", out.id());
    			invest.put("v_id", out.value("v_id"));
    			invest.put("v_pids", Lists.newArrayList(out.values("v_pids")));
    			invest.put("v_type", out.value("v_type"));
    			invest.put("name", out.value("name"));
    			investList.add(invest);
    			finance.put("investPartyList", investList);
    			
    			// 融资方
    			Vertex in = e.inVertex();
    			List<Map<String, Object>> companyList = new ArrayList<>();
    			Map<String, Object> ent = new HashMap<>();
    			ent.put("id", in.id());
    			ent.put("v_id", in.value("v_id"));
    			ent.put("v_pids", Lists.newArrayList(in.values("v_pids")));
    			ent.put("v_type", in.value("v_type"));
    			ent.put("name", in.value("name"));
    			companyList.add(ent);
    			finance.put("financedPartyList", companyList);
    			
    			// 融资标签
    			finance.put("tags", e.property("e_tags").isPresent() ? e.value("e_tags") : "");
    			
    			//
    			bw.write(gson.toJson(finance));
    			bw.newLine();
    		}
    		
    		LOGGER.info("export finance edge done.");
    		
    	}
    	
    }
    
    public EntKgExport(TitanGraph graph) {
		this.g = graph.traversal();
	}

    private final GraphTraversalSource g;
    
    private static final Logger LOGGER = Logger.getLogger(EntKgExport.class);
}
