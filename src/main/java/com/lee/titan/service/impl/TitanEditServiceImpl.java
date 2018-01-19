package com.lee.titan.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.lee.titan.type.FieldType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.lee.titan.service.TitanEditService;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.schema.TitanManagement.IndexBuilder;

public class TitanEditServiceImpl implements TitanEditService {
	
	/**
	 * 图的引用
	 */
	private TitanGraph graph;
	
	/**
	 * 图遍历
	 */
	private GraphTraversalSource g;
	
	public TitanEditServiceImpl(TitanGraph graph) {
		this.graph = graph;
		g = graph.traversal();
	}
	
	public TitanGraph titanGraph() {
		return graph;
	}
	
	public TitanManagement titanManagement() {
		return graph.openManagement();
	}
	
	public GraphTraversalSource graphTraversalSource() {
		return g;
	}
		
	public VertexLabel addVertexLabel(TitanManagement mgmt, String label) {
		return mgmt.makeVertexLabel(label).make();
	}
	
	public VertexLabel vertexLabel(TitanManagement mgmt, String label) {
		return mgmt.getVertexLabel(label);
	}
	
	public void removeVertexLabel(TitanManagement mgmt, String label) {
		VertexLabel vertexLabel = mgmt.getVertexLabel(label);
		if (null != vertexLabel) vertexLabel.remove();
	}
	
	public EdgeLabel addEdgeLabel(TitanManagement mgmt, String label, String multiplicity) {
		if (null == multiplicity || multiplicity.length() == 0) multiplicity = "multi";;
		return mgmt.makeEdgeLabel(label).multiplicity(Multiplicity.valueOf(multiplicity.toUpperCase())).make();
	}
	
	public void removeEdgeLabel(TitanManagement mgmt, String label) {
		EdgeLabel edgeLabel = mgmt.getEdgeLabel(label);
		if (null != edgeLabel) edgeLabel.remove();
	}
	
	public PropertyKey addPropertyKey(TitanManagement mgmt, String fname, String ftype, String cardinality) {
		Class<?> fclass = getClassByAlias(FieldType.valueOf(ftype.toUpperCase()));
		return mgmt.makePropertyKey(fname).dataType(fclass).cardinality(Cardinality.valueOf(cardinality.toUpperCase())).make();
	}
	
	public PropertyKey getPropertyKey(TitanManagement mgmt, String fname) {
		return mgmt.getPropertyKey(fname);
	}
	
	public void removePropertyKey(TitanManagement mgmt, String fname) {
		PropertyKey pk = mgmt.getPropertyKey(fname);
		if (null != pk) pk.remove();
	}
	
	public void buildMixedIndex(TitanManagement mgmt, String backingIndex, String mappingType,
			Class<? extends Element> clazz, String indexOnlyLabel, PropertyKey... pks) {
		IndexBuilder indexBuilder = mgmt.buildIndex(mappingType, clazz);
		if (null != pks && pks.length > 0) {
			for (PropertyKey pk : pks) {
				indexBuilder.addKey(pk);
			}
		} 
		
		if (null != indexOnlyLabel && indexOnlyLabel.length() > 0) {
			if (clazz == Vertex.class) {
				indexBuilder.indexOnly(mgmt.getVertexLabel(indexOnlyLabel));
			} else if (clazz == Edge.class) {
				indexBuilder.indexOnly(mgmt.getEdgeLabel(indexOnlyLabel));
			}
		}
		
		indexBuilder.buildMixedIndex(backingIndex);
	}
	
	public void buildMixedIndex(TitanManagement mgmt, String backingIndex, String mappingType, Class<? extends Element> clazz,
			String indexOnlyLabel, String... fts) {
		
		IndexBuilder indexBuilder = mgmt.buildIndex(mappingType, clazz);
		
		if (null != fts && fts.length > 0) {
			for (int i = 0; i < fts.length; i = i+ 2) {
				String fname = fts[i];// 字段名
				String ftype = fts[i + 1];// 字段类型
				
				Class<?> fclass = getClassByAlias(ftype);
				
				PropertyKey pk = mgmt.makePropertyKey(fname).dataType(fclass).make();
				// indexBuilder.addKey(pk, Parameter.of("mapping", Mapping.STRING));
				// indexBuilder.addKey(pk, Mapping.STRING.asParameter());
				indexBuilder.addKey(pk);
			}
		}
		
		//
		if (null != indexOnlyLabel && indexOnlyLabel.length() > 0) {
			if (clazz == Vertex.class) {
				indexBuilder.indexOnly(mgmt.getVertexLabel(indexOnlyLabel));
			} else if (clazz == Edge.class) {
				indexBuilder.indexOnly(mgmt.getEdgeLabel(indexOnlyLabel));
			}
		}
		//
		indexBuilder.buildMixedIndex(backingIndex);
	}
	
	public void addIndexKey(TitanManagement mgmt, TitanGraphIndex existsIndex, PropertyKey pk) {
		mgmt.addIndexKey(existsIndex, pk);
	}
	
	public Vertex addVertex(String labelValue, long vid, int vtype, Collection<Long> vpid, String name, Object... kvs) {
//		Vertex vertex = addVertex("v_id", vid, "v_type", vtype, "name", name);
//		if (null != vpid && vpid.size() > 0) {
//			for (Long pid : vpid) {
//				vertex.property("v_pids", pid);
//			}
//		}
//		return vertex;
		
		//
		List<Object> keyValues = new ArrayList<>();
		if (null != labelValue && labelValue.length() > 0) {
			keyValues.add(T.label);
			keyValues.add(labelValue);
		}
		keyValues.add("v_id");
		keyValues.add(vid);
		keyValues.add("v_type");
		keyValues.add(vtype);
		keyValues.add("name");
		keyValues.add(name);
		if (null != vpid && vpid.size() > 0) {
			for (Long pid : vpid) {
				keyValues.add("v_pids");
				keyValues.add(pid);
			}
		}
		
		if (null != kvs && kvs.length > 0) {
			for (int i = 0; i < kvs.length; i = i + 2) {
				keyValues.add(kvs[i]);// key
				keyValues.add(kvs[i + 1]);// value
			}
		}
		
		return graph.addVertex(keyValues.toArray());
	}
	
//	public Vertex addVertex(String labelValue, Object... keyValues) {
//	TitanVertex vertex = graph.addVertex(labelValue);
//	com.thinkaurelius.titan.graphdb.util.ElementHelper.attachProperties(vertex, keyValues);
//	return vertex;
//}
	
	public Vertex getVertex(String label, long vid, long vpid, int vtype, String name) {
		Vertex res = null;
		GraphTraversal<Vertex, Vertex> vs = null;
		if (vid > -1 && vtype > -1 && null != name && name.length() > 0) {
			 vs = g.V().has("v_id", vid).has("v_type", vtype).has("name", name);
		} else if (vid > -1 && vtype > -1) {
			 vs = g.V().has("v_id", vid).has("v_type", vtype);
		} else if (vid > -1 && null != name && name.length() > 0) {
			 vs = g.V().has("v_id", vid).has("name", name);
		} else if (vtype > -1 && null != name && name.length() > 0) {
			 vs = g.V().has("v_type", vtype).has("name", name);
		} else if (vid > -1) {
			 vs = g.V().has("v_id", vid);
		} else if (null != name && name.length() > 0) {
			 vs = g.V().has("name", name);
		} else {
			return null;
		}
		
		// 检查label
		if (null != label && label.length() > 0) {
			vs.hasLabel(label);
		}
		
		// 父概念
		if (vpid > -1) {
			vs.has("v_pids", vpid);
		}
		
		if (vs.hasNext()) res = vs.next();;
		
		return res;
	}
	
	public void removeVertex(String label, long vid, long vpid, int vtype, String name) {
		GraphTraversal<Vertex, Vertex> vs = null;
		if (vid > -1 && vtype > -1 && null != name && name.length() > 0) {
			 vs = g.V().has("v_id", vid).has("v_type", vtype).has("name", name);
		} else if (vid > -1 && vtype > -1) {
			 vs = g.V().has("v_id", vid).has("v_type", vtype);
		} else if (vid > -1 && null != name && name.length() > 0) {
			 vs = g.V().has("v_id", vid).has("name", name);
		} else if (vtype > -1 && null != name && name.length() > 0) {
			 vs = g.V().has("v_type", vtype).has("name", name);
		} else if (vid > -1) {
			 vs = g.V().has("v_id", vid);
		} else if (null != name && name.length() > 0) {
			 vs = g.V().has("name", name);
		} else {
			return;
		}
		
		// 检查label
		if (null != label && label.length() > 0) {
			vs.hasLabel(label);
		}
		
		// 父概念
		if (vpid > -1) {
			vs.has("v_pids", vpid);
		}
		
		
		vs.drop().iterate();
		// if (vs.hasNext()) vs.next().remove();
	}
	
	public Edge addEdge(String labelValue, Vertex outVertex, Vertex inVertex, Object... keyValues) {
		if (null == outVertex || null == inVertex) {
			LOGGER.error("there has one or all vertex not exists when add edge.");
			return null;
		}
		
		// 边已经存在了
//		if (g.V(outVertex.id()).out(labelValue).hasId(inVertex.id()).hasNext()) {
//			LOGGER.error("the edge already exists.");
//			return null;
//		}
		
		return outVertex.addEdge(labelValue, inVertex, keyValues);
	}
	
	public Edge addEdge(String labelValue, long outVid, long inVid, Object... keyValues) {
		Vertex outVertex = vertex(outVid);
		Vertex inVertex = vertex(inVid);
		return addEdge(labelValue, outVertex, inVertex, keyValues);
	}
	
	public void removeEdge(String labelValue, long outVid, long inVid) {
		GraphTraversal<Vertex, Edge> es = g.V().has("v_id", outVid).outE(labelValue).where(__.otherV().has("v_id", inVid));
		if (es.hasNext()) {
			es.next().remove();
		}
	}
	
	public void addSubClass(long pid, long vid, Object... keyValues) {
		Vertex p = vertex(pid);
		Vertex v = vertex(vid);
		
		if (null == p || null == v) return;
		
		p.addEdge("subClass", v, keyValues);
	}
	
	public void addInstance(long pid, long vid, Object... keyValues) {
		Vertex p = vertex(pid);
		Vertex v = vertex(vid);
		
		if (null == p || null == v) return;
		
		p.addEdge("instance", v, keyValues);
	}
	
	public Vertex vertex(long vid) {
		Vertex v = null;
		GraphTraversal<Vertex, Vertex> vt = g.V().has("v_id", vid);
		if (vt.hasNext()) v = vt.next();;
		return v;
	}
	
	private Class<?> getClassByAlias(FieldType ftype) {
		Class<?> clazz = null;
		switch (ftype) {
		case CHAR:
			clazz = Character.class;
			break;
		case STRING:
			clazz = String.class;
			break;
		case BYTE:
			clazz = Byte.class;
			break;
		case SHORT:
			clazz = Short.class;
			break;
		case INT:
			clazz = Integer.class;
			break;
		case LONG:
			clazz = Long.class;
			break;
		case BIGINT:
			clazz = Long.class;
			break;
		default: 
			break;
		}
		return clazz;
	}
	
	private Class<?> getClassByAlias(String alias) {
		Class<?> clazz = null;
		switch (alias) {
		case "string":
			clazz = String.class;
			break;
		case "int":
			clazz = Integer.class;
			break;
		case "long":
			clazz = Long.class;
			break;
		}
		return clazz;
	}

}
