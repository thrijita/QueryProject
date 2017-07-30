package com.niit.csvparse;
import java.util.*;

public class DataSetter
{
	private List<RowData> resultSet=new ArrayList<RowData>();
	
	private LinkedHashMap<String,Float> aggregateRow=new LinkedHashMap<String,Float>();
	
	private LinkedHashMap<String,List<RowData>> groupByDataSetNew=new LinkedHashMap<String,List<RowData>>();
	
	private LinkedHashMap<String,LinkedHashMap<String,Float>> totalGroupedData=new LinkedHashMap<String,LinkedHashMap<String,Float>>(); 
	
	public LinkedHashMap<String,LinkedHashMap<String, Float>> getTotalGroupedData() 
	{
		return totalGroupedData;
	}

	public void setTotalGroupedData(LinkedHashMap<String,LinkedHashMap<String, Float>> totalGroupedData) 
	{
		this.totalGroupedData = totalGroupedData;
	}

	public LinkedHashMap<String, List<RowData>> getGroupByDataSetNew() 
	{
		return groupByDataSetNew;
	}

	public void setGroupByDataSetNew(LinkedHashMap<String, List<RowData>> groupByDataSetNew) 
	{
		this.groupByDataSetNew = groupByDataSetNew;
	}

	public LinkedHashMap<String, Float> getAggregateRow() {
		return aggregateRow;
	}

	public void setAggregateRow(LinkedHashMap<String, Float> aggregateRow) {
		this.aggregateRow = aggregateRow;
	}

	public List<RowData> getResultSet() 
	{
		return resultSet;
	}

	public void setResultSet(List<RowData> resultSet) 
	{
		this.resultSet = resultSet;
	}
	
	
}
