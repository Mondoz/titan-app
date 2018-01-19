package com.lee.titan.domain;

import java.util.Map;

public class TitanPath {
	
	public final long outId;
	public final String outName;
	public final String outLabel;
	
	public final long inId;
	public final String inName;
	public final String inLabel;
	
	public final String elabel;
	public final Map<String, Object> eproperties;
	
	//
	public TitanPath(long outId, String outName, String outLabel, long inId, String inName, String inLabel,
			String elabel, Map<String, Object> eproperties) {
		this.outId = outId;
		this.outName = outName;
		this.outLabel = outLabel;
		this.inId = inId;
		this.inName = inName;
		this.inLabel = inLabel;
		this.elabel = elabel;
		this.eproperties = eproperties;
	}
	
	//
	public boolean equals(Object obj) {
		TitanPath o = (TitanPath) obj;
		return (outId + inId + elabel).equals(o.outId + o.inId + o.elabel);
	}
	
	public int hashCode() {
		return (outId + inId + elabel).hashCode();
	}
	
	/*
	{
	    "inLabel": "enterprise", 
	    "inId": 4599984, 
	    "e_financed_date": "F轮-上市前", 
	    "e_financed_amount": "30亿美元", 
	    "e_financed_round": "F轮-上市前", 
	    "outId": 102932728, 
	    "outName": "阿里巴巴", 
	    "e_label": "投资企业", 
	    "outLabel": "investment", 
	    "inName": "北京小桔科技有限公司"
	}
	*/
}
