package com.lee.titan.service;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public interface TitanEditService {
	
	Logger LOGGER = Logger.getLogger(TitanEditService.class);
	
	TitanGraph titanGraph();
	
	TitanManagement titanManagement();
	
	GraphTraversalSource graphTraversalSource();
	
	/**
	 * 添加顶点的label
	 * @param mgmt
	 * @param label
	 * @return
	 */
	VertexLabel addVertexLabel(TitanManagement mgmt, String label);
	
	/**
	 * 
	 * @param mgmt
	 * @param label
	 * @return
	 */
	VertexLabel vertexLabel(TitanManagement mgmt, String label);
	
	/**
	 * 删除顶点的label
	 * @param mgmt
	 * @param label
	 */
	void removeVertexLabel(TitanManagement mgmt, String label);
	
	/**
	 * 添加边的label
	 * @param mgmt
	 * @param label
	 * @param multiplicity
	 * @return
	 */
	EdgeLabel addEdgeLabel(TitanManagement mgmt, String label, String multiplicity);
	
	/**
	 * 删除边的label
	 * @param mgmt
	 * @param label
	 */
	void removeEdgeLabel(TitanManagement mgmt, String label);
	
	/**
	 * 定义titan的属性 顶点和边都可以使用
	 * @param mgmt
	 * @param fname
	 * @param ftype
	 * @param cardinality
	 * @return
	 */
	PropertyKey addPropertyKey(TitanManagement mgmt, String fname, String ftype, String cardinality);
	
	PropertyKey getPropertyKey(TitanManagement mgmt, String fname);
	
	/**
	 * 删除titan定义的属性
	 * @param mgmt
	 * @param fname
	 */
	void removePropertyKey(TitanManagement mgmt, String fname);
	
	void buildMixedIndex(TitanManagement mgmt, 
			String backingIndex, 
			String mappingType, 
			Class<? extends Element> clazz, 
			String indexOnlyLabel, 
			PropertyKey... pks);
	
	void buildMixedIndex(TitanManagement mgmt, 
			String backingIndex, 
			String mappingType, 
			Class<? extends Element> clazz, 
			String indexOnlyLabel, 
			String... fts);
	
	/**
	 * 给已经存在的索引 添加一个field
	 * @param mgmt
	 * @param existsIndex
	 * @param pk
	 */
	void addIndexKey(TitanManagement mgmt, TitanGraphIndex existsIndex, PropertyKey pk);
	
	/**
	 * 添加一个顶点
	 * @param vid
	 * @param vtype
	 * @param vpid
	 * @param name
	 * @return
	 */
	Vertex addVertex(String labelValue, long vid, int vtype, Collection<Long> vpid, String name, Object... keyValues);
	
	Vertex getVertex(String label, long vid, long vpid, int vtype, String name);
	
	void removeVertex(String label, long vid, long vpid, int vtype, String name);
	
	/**
	 * 添加一条边
	 * @param labelValue
	 * @param outVertex
	 * @param inVertex
	 * @param keyValues
	 * @return
	 */
	Edge addEdge(String labelValue, Vertex outVertex, Vertex inVertex, Object... keyValues);
	
	Edge addEdge(String labelValue, long outVid, long inVid, Object... keyValues);
	
	void removeEdge(String labelValue, long outVid, long inVid);
	
	/**
	 * 添加一条子类边
	 * @param pid
	 * @param vid
	 * @param keyValues
	 */
	void addSubClass(long pid, long vid, Object... keyValues);
	
	/**
	 * 添加一条实例边
	 * @param pid
	 * @param vid
	 * @param keyValues
	 */
	void addInstance(long pid, long vid, Object... keyValues);
	
	/**
	 * 通过vid获取顶点
	 * @param vid
	 * @return
	 */
	Vertex vertex(long vid);
	
}
