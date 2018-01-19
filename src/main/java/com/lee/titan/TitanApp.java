package com.lee.titan;


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class TitanApp {
	
	public static void main(String[] args) {
		
		TitanGraph graph = TitanFactory.open("conf/ent-ca-es.properties");
		
		
		TitanManagement mgmt = graph.openManagement();
		mgmt.makeVertexLabel("person").make();
		// ONE2MANY: 指的是每个顶点 只能有一条这个label的入度边   可以有多个这个label的出度边
		// MANY2ONE: 指的是每个顶点 只能有一条这个label的出度边   可以有多个这个label的入度边
		mgmt.makeEdgeLabel("staff").multiplicity(Multiplicity.ONE2MANY).make();
		PropertyKey kgId = mgmt.makePropertyKey("kg_id").dataType(Integer.class).make();
		PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
		PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
		PropertyKey desc = mgmt.makePropertyKey("desc").dataType(String.class).make();
		mgmt.buildIndex("vertex", Vertex.class)
		.addKey(kgId)
		.addKey(age)
		.addKey(name, Mapping.STRING.asParameter())
		.addKey(desc, Mapping.TEXT.asParameter())
		.buildMixedIndex("search");
		mgmt.commit();
		
		GraphTraversalSource g = graph.traversal();
		GraphTraversal<Vertex, Vertex> bosss = g.V().has("desc", Text.textContains("政治"));
		while (bosss.hasNext()) {
			Vertex v = bosss.next();
			System.out.println(v.label() + " " + v.id() + " " + v.property("kg_id").value() + " " + v.property("name").value());
		}
		System.out.println("----------------------------------");

		graph.close();
	}
}
