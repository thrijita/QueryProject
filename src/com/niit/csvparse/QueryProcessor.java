package com.niit.csvparse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class QueryProcessor 
{
	QueryParameter queryParam;
	
	public QueryProcessor(QueryParameter queryParam)
	{
		this.queryParam=queryParam;
	}
	
	//selecting for particular columns
	public DataSetter getDataSetSimpleQuery()throws Exception
	{
		DataSetter dataSet=new DataSetter();
		RowHeader headerRow=queryParam.getHeaderRow();
		
			BufferedReader bufferedReader=new BufferedReader(new FileReader(queryParam.getFilePath()));
			RowData rowData;
			bufferedReader.readLine();
			String row;
			while((row=bufferedReader.readLine())!=null)
			{
				int count=0;
				rowData=new RowData();
				
				String rowValues[]=row.trim().split(",");
				int columnCount=rowValues.length;
				
				if(!queryParam.isHasAllColumn())
				{
					Set<String> columnNames=headerRow.keySet();
					for(String columnName:queryParam.getColumNames().getColumns())
					{
						for(String actualColumnName:columnNames)
						{
							if(actualColumnName.equals(columnName))
							{
								rowData.put(headerRow.get(columnName),rowValues[headerRow.get(columnName)].trim());
							}
						}
					}
				}
				else
				{
					while(count<columnCount)
					{
						rowData.put(count,rowValues[count].trim());
						count++;
					}
				}
				
				if(queryParam.isHasWhere())
				{
					if(evaluateWhereCondition(queryParam,rowValues))
					{
						dataSet.getResultSet().add(rowData);
						
						if(queryParam.isHasGroupBy())
						{
							String groupByColumnValue=rowData.get(headerRow.get(queryParam.getGroupByColumn()));
							List<RowData> dataValues=null;
							if(dataSet.getGroupByDataSetNew().containsKey(groupByColumnValue))
							{
								dataValues=dataSet.getGroupByDataSetNew().get(groupByColumnValue);
								dataValues.add(rowData);
							}
							else
							{
								dataValues=new ArrayList<RowData>();
								dataValues.add(rowData);
							}
							dataSet.getGroupByDataSetNew().put(groupByColumnValue,dataValues);
						}
					}
				}
				else
				{
					dataSet.getResultSet().add(rowData);
					
					if(queryParam.isHasGroupBy())
					{
						String groupByColumnValue=rowData.get(headerRow.get(queryParam.getGroupByColumn()));
						List<RowData> dataValues=null;
						if(dataSet.getGroupByDataSetNew().containsKey(groupByColumnValue))
						{
							dataValues=dataSet.getGroupByDataSetNew().get(groupByColumnValue);
							dataValues.add(rowData);
						}
						else
						{
							dataValues=new ArrayList<RowData>();
							dataValues.add(rowData);
						}
						dataSet.getGroupByDataSetNew().put(groupByColumnValue,dataValues);
					}
				}
				
			
			}
		
			if(queryParam.isHasOrderBy())
			{
				SortData sortData=new SortData();
				sortData.setSortingIndex(queryParam.getHeaderRow().get(queryParam.getOrderByColumn()));
				Collections.sort(dataSet.getResultSet(),sortData);
			}
			
			if(queryParam.isHasAggregate())
			{
				if(queryParam.isHasGroupBy())
				{
					LinkedHashMap<String,List<RowData>> groupDataSet=dataSet.getGroupByDataSetNew();
					Set<String> groupByColumnValues=groupDataSet.keySet(); 
					LinkedHashMap<String,Float> eachGroupAggregateRow;
					for(String groupByColumnValue:groupByColumnValues)
					{
						List<RowData> groupRows=dataSet.getGroupByDataSetNew().get(groupByColumnValue);
						eachGroupAggregateRow=evaluateAggregateColumns(groupRows,queryParam);
						dataSet.getTotalGroupedData().put(groupByColumnValue,eachGroupAggregateRow);
					}
					
				}
				else
				{
					dataSet.setAggregateRow(evaluateAggregateColumns(dataSet.getResultSet(),queryParam));
				}
			}
			
			return dataSet;
	}
	//selecting using aggregate functions and group by done here
	public LinkedHashMap<String,Float> evaluateAggregateColumns(List<RowData> groupRows,QueryParameter queryParam)
	{
		LinkedHashMap<String,Float> eachGroupRow=new LinkedHashMap<String,Float>();
		
		int aggregateColumnCount=queryParam.getColumNames().getColumns().size();
		List<String> aggregateColumns=queryParam.getColumNames().getColumns();
		int count=0,recordCount=0,columnCount=0;
		float sumValue=0,avgValue=0,minValue=0,maxValue=0,countValue=0,countColumnValue=0;
		
		int groupRowSize=groupRows.size();
		
		while(count<groupRowSize)
		{
			columnCount=0;	
			while(columnCount<aggregateColumnCount)
			{
			String aggregateColumnString=aggregateColumns.get(columnCount);
			String aggregateColumnName,actualRowValue;
			
			if(!aggregateColumnString.equals(queryParam.getGroupByColumn()))
			{
			aggregateColumnName=aggregateColumnString.substring(aggregateColumnString.indexOf('(')+1,aggregateColumnString.indexOf(')'));
			actualRowValue=groupRows.get(count).get(queryParam.getHeaderRow().get(aggregateColumnName));
			
				if(!evaluateDataType(actualRowValue))
				{
					float rowValue=Float.parseFloat(actualRowValue);
					
					if(count==0)
					{
						minValue=Float.parseFloat(actualRowValue);
						maxValue=Float.parseFloat(actualRowValue);
					}
					
					if(aggregateColumnString.contains("sum("))
					{
						if(eachGroupRow.containsKey(aggregateColumnString))
						{
							sumValue=eachGroupRow.get(aggregateColumnString);
							sumValue=sumValue+rowValue;
							eachGroupRow.put(aggregateColumnString,sumValue);
						}
						else
						{
							eachGroupRow.put(aggregateColumnString,rowValue);
						}
					}
					else if(aggregateColumnString.contains("min("))
					{
							if(minValue>rowValue)
							{
								minValue=rowValue;
							}
							eachGroupRow.put(aggregateColumnString,minValue);
					}
					else if(aggregateColumnString.contains("max("))
					{
							if(maxValue<rowValue)
							{
								maxValue=rowValue;
							}
							eachGroupRow.put(aggregateColumnString,maxValue);
					}
				}
				else if(aggregateColumnString.contains("count(*)"))
				{
					countValue++;
					eachGroupRow.put(aggregateColumnString,countValue);
				}
				
				else if(aggregateColumnString.contains("count("))
				{
					countColumnValue++;
					eachGroupRow.put(aggregateColumnString,countColumnValue);
				}
			 }
				
			columnCount++;
			}
			count++;
		}
		
		return eachGroupRow;
	}
	private boolean evaluateWhereCondition(QueryParameter queryParam,String rowValues[])
	{
		List<RestrictionCondition> listRelationalExpression=queryParam.getListrelexpr();
		RowHeader headerRow=queryParam.getHeaderRow();
		List<String> logicalOperators=queryParam.getLogicalOperator();
		
		boolean expression=evaluateEachCondition(listRelationalExpression.get(0),rowValues,headerRow);
		
		int logicalOperatorCount=logicalOperators.size();
		int count=0;
		
		if(logicalOperatorCount>0)
		{
			while(count<logicalOperatorCount)
			{
				if(logicalOperators.get(count).equals("and"))
				{
					expression=expression && evaluateEachCondition(listRelationalExpression.get(count+1),rowValues,headerRow);
				}
				else
				{
					expression=expression || evaluateEachCondition(listRelationalExpression.get(count+1),rowValues,headerRow);
				}
				
				count++;
			}
		}
		
		return expression;
	}
	
	private boolean evaluateEachCondition(RestrictionCondition condition,String rowValues[],RowHeader headerRow)
	{
		
		boolean expression=false;
		
		String conditionColumnName=condition.getColumn().trim().toLowerCase();
		String conditionOperator=condition.getOperator();
		String conditionValue=condition.getValue();
			
			String columnValue=rowValues[headerRow.get(conditionColumnName)].trim();
			boolean isString=evaluateDataType(columnValue);
				
				if(isString)
				{
					if(conditionOperator.equals("="))
					{
						if(columnValue.equals(conditionValue))
						{
							expression=true;
						}
					}
					else if(conditionOperator.equals("!="))
					{
						if(!columnValue.equals(conditionValue))
						{
							expression=true;
						}
					}
				}
				else
				{
					float conditionParseValue=Float.parseFloat(conditionValue);
					float rowDataValue=Float.parseFloat(rowValues[queryParam.getHeaderRow().get(conditionColumnName)]);
					switch(conditionOperator)
					{
						case ">=":
							expression=rowDataValue>=conditionParseValue;
							break;
						case "<=":
							expression=rowDataValue<=conditionParseValue;
							break;
						case ">":
							expression=rowDataValue>conditionParseValue;
							break;
						case "<":
							expression=rowDataValue<conditionParseValue;
							break;
						case "=":
							expression=rowDataValue==conditionParseValue;
							break;
						case "!=":
							expression=rowDataValue!=conditionParseValue;
							break;
					}
				}
		
		return expression;
	}
	
	private boolean evaluateDataType(String value)
	{
		try
		{
			Float f=Float.parseFloat(value);
			return false;
		}
		catch(Exception e)
		{
			return true;
		}
	}
	
}
