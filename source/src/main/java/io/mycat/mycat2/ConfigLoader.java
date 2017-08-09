/*
 * Copyright (c) 2016, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.mycat2;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import io.mycat.mycat2.beans.*;
/**
 * Load mycat config
 * @author wuzhihui
 *
 */
public class ConfigLoader {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigLoader.class);
	
	public ConfigLoader() {
		
	}
	
	 public static List<SchemaBean> loadSheamBeans(String schemaBeanuri){ 
		 List<SchemaBean>  list=new LinkedList<SchemaBean>();  
		 try{    
	            NodeList nodeList=loadXmlDoc(schemaBeanuri).getElementsByTagName("schema");    
	            for(int i=0;i<nodeList.getLength();i++){   
	            	Node curRuleNode=nodeList.item(i);
	            	
	            	NamedNodeMap map=curRuleNode.getAttributes();
	            	String name=getAttribute(map,"name",null);
	            	String schemaType=getAttribute(map,"nopartion","true");
	            	String defaultDB=getAttribute(map,"default-db",null);
	            	String[] dnItems=defaultDB.split(":");
	            	DNBean dnBean=new DNBean(dnItems[0].trim(),dnItems[1].trim());
	            	List<Node> tableNodes=getChildNodes(curRuleNode,"table");
	            	List<TableDefBean>  tableLst=new LinkedList<TableDefBean>(); 
	            	tableNodes.stream().forEach(node->{
	            		NamedNodeMap attrs = node.getAttributes();
	            		String tName=getAttribute(attrs,"name",null);
	            		int tType=getIntAttribute(attrs,"type",0);
	            		String tKey=getAttribute(attrs,"sharding-key",null);
	            		String tRule=getAttribute(attrs,"sharding-rule",null);
	            		TableDefBean tbBean=new TableDefBean(tName,tType,tKey,tRule);
	            		tableLst.add(tbBean);});
	            	SchemaBean sBean=new SchemaBean(name,dnBean,("true".equalsIgnoreCase(schemaType)),tableLst);
	            	LOGGER.debug("schema-bean: {}", sBean);
	            	list.add(sBean);
	            }    
	        }catch(Exception e){    
	        	LOGGER.warn("loadSheamBeans err "+e );
	        }    
	        return list;     
		 
	 }
	
	 public static List<ShardingRuleBean> loadShardingRules(String shardingruleuri){ 
		 List<ShardingRuleBean>  list=new LinkedList<ShardingRuleBean>();  
		 try{    
             
	            NodeList nodeList=loadXmlDoc(shardingruleuri).getElementsByTagName("rule");    
	            for(int i=0;i<nodeList.getLength();i++){   
	            	Node curRuleNode=nodeList.item(i);
	            	
	            	NamedNodeMap map=curRuleNode.getAttributes();
	            	String name=getAttribute(map,"name",null);
	            	String algorithm=getAttribute(map,"algorithm",null);
	            	List<Node> paramNodes=getChildNodes(curRuleNode,"param");
	            	Map<String,String> params=new HashMap<String,String>();
	            	paramNodes.stream().forEach(node->{
	            		NamedNodeMap attrs = node.getAttributes();
	            		String paramName=getAttribute(attrs,"name",null);
	            		String paramVal=getValue(node.getFirstChild(),null);
	            		params.put(paramName, paramVal);});
	                 ShardingRuleBean ruleBean=new ShardingRuleBean(name,algorithm,params);
	                 list.add(ruleBean);
	            }    
	        }catch(Exception e){    
	        	LOGGER.warn("loadShardingRules err "+e );
	        }    
	        return list;     
		 
	 }
	/**
	 * load datasource file
	 * @param datasourceuri
	 * @return
	 */
    public static List<MySQLRepBean> loadMySQLRepBean(String datasourceuri){ 
    	 List<MySQLRepBean>  list=new LinkedList<MySQLRepBean>();  
        try{    
              
            NodeList nodeList=loadXmlDoc(datasourceuri).getElementsByTagName("mysql-replica");    
            for(int i=0;i<nodeList.getLength();i++){   
            	Node curRepNode=nodeList.item(i);
            	
            	NamedNodeMap map=curRepNode.getAttributes();
            	String name=getAttribute(map,"name",null);
            	int type=getIntAttribute(map,"type",0);
            	int switchType=getIntAttribute(map,"switch-type",0);
            	MySQLRepBean repBean=new MySQLRepBean(name,type);
            	repBean.setSwitchType(switchType);
            	List<Node> mysqlNodes=getChildNodes(curRepNode,"mysql");
            	List<MySQLBean> allMysqls=mysqlNodes.stream().map(mysqlNode->{  
            	NamedNodeMap attrs = mysqlNode.getAttributes();
        		String ip=getAttribute(attrs,"ip",null);
        		String user=getAttribute(attrs,"user",null);
        		String password=getAttribute(attrs,"password",null);
        		int port=getIntAttribute(attrs,"port",3306);
        		 MySQLBean mysql=new MySQLBean(ip,port,user,password);
        		 return mysql;}).collect(Collectors.toList()) ;
                 repBean.setMysqls(allMysqls);
                 list.add(repBean);
            }    
        }catch(Exception e){    
        	LOGGER.warn("loadMySQLRepBean err "+e );
        }    
        return list;    
    }   
    private static Document loadXmlDoc(String uri) throws Exception
    {
    	DocumentBuilderFactory dbf=DocumentBuilderFactory.newInstance();    
        DocumentBuilder db=dbf.newDocumentBuilder();    
        Document doc=db.parse(uri);  
        return doc;
    }
    private static String getAttribute(NamedNodeMap map,String attr,String defaultVal)
    {
    	return getValue(map.getNamedItem(attr),defaultVal);
    }
    private static int getIntAttribute(NamedNodeMap map,String attr,int defaultVal)
    {
    	return getIntValue(map.getNamedItem(attr),defaultVal);
    }
    private static String getValue(Node node,String defaultVal)
    {
    	return node==null?defaultVal:node.getNodeValue();
    }
    private static int getIntValue(Node node,int defaultVal)
    {
    	return node==null?defaultVal:Integer.valueOf(node.getNodeValue());
    }
    private static List<Node> getChildNodes(Node theNode,String childElName)
    {
    	 LinkedList<Node> reslt=new LinkedList<Node>();
    	 NodeList childs=theNode.getChildNodes();
    	 for(int j=0;j<childs.getLength();j++){  
    		 if(childs.item(j).getNodeType()==Document.ELEMENT_NODE && childs.item(j).getNodeName().equals(childElName))
    		 {
    			 reslt.add(childs.item(j)) ;
    		 }
    	 }
    	 return reslt;
    }
}
