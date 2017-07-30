package com.niit.csvparse;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//class to parse the given query into parts
public class QueryParameter 
{
	private String filePath;
	private String orderByColumn,groupByColumn;
	private SelectColumns columNames;
	private List<RestrictionCondition> restrictions;
	private boolean hasGroupBy=false,hasOrderBy=false,hasWhere=false,hasAllColumn=false,hasColumn=false,hasSimpleQuery,hasAggregate=false;
	private List<String> logicalOperator;
	private RowHeader headerRow;
	
	
	public QueryParameter(String queryString)throws Exception
	{
		
		columNames=new SelectColumns();
		restrictions=new ArrayList<RestrictionCondition>();
		logicalOperator=new ArrayList<String>();
		this.parseQuery(queryString);
		headerRow=this.setHeaderRow();
	}
	
	
	public QueryParameter parseQuery(String queryString)
	{
		String baseQuery=null,conditionQuery=null,selectcol=null;
		
		if(queryString.contains("order by"))
		{
			baseQuery=queryString.split("order by")[0].trim();
			orderByColumn=queryString.split("order by")[1].trim().toLowerCase();
			if(baseQuery.contains("where"))
			{
				conditionQuery=baseQuery.split("where")[1].trim();
				this.relationalExpressionProcessing(conditionQuery);
				baseQuery=baseQuery.split("where")[0].trim();
				hasWhere=true;
			}
			filePath=baseQuery.split("from")[1].trim();
			baseQuery=baseQuery.split("from")[0].trim();
			selectcol=baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasOrderBy=true;
		}
		else if(queryString.contains("group by"))
		{
			baseQuery=queryString.split("group by")[0].trim();
			groupByColumn=queryString.split("group by")[1].trim().toLowerCase();
			if(baseQuery.contains("where"))
			{
				conditionQuery=baseQuery.split("where")[1].trim();
				this.relationalExpressionProcessing(conditionQuery);
				baseQuery=baseQuery.split("where")[0].trim();
				hasWhere=true;
			}
			filePath=baseQuery.split("from")[1].trim();
			baseQuery=baseQuery.split("from")[0].trim();
			selectcol=baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasGroupBy=true;
		}
		else if(queryString.contains("where"))
		{
			baseQuery=queryString.split("where")[0];
			conditionQuery=queryString.split("where")[1];
			conditionQuery=conditionQuery.trim();
			filePath=baseQuery.split("from")[1].trim();
			baseQuery=baseQuery.split("from")[0].trim();
			this.relationalExpressionProcessing(conditionQuery);
			selectcol=baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasWhere=true;
		}
		else
		{
			baseQuery=queryString.split("from")[0].trim();
			filePath=queryString.split("from")[1].trim();
			selectcol=baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasSimpleQuery=true;
		}
		
		return this;	
	}
	
	private void relationalExpressionProcessing(String conditionQuery)
	{
		String oper[]={">=","<=",">","<","!=","="};
		
		String relationalQueries[]=conditionQuery.split("\\s+and\\s+|\\s+or\\s+");
		
		for(String relationQuery:relationalQueries)
		{	
			relationQuery=relationQuery.trim();
			for(String operator:oper) // avoid this loop
			{
				if(relationQuery.contains(operator))
				{
					RestrictionCondition restrictcond=new RestrictionCondition();
					restrictcond.setColumn(relationQuery.split(operator)[0].trim());
					restrictcond.setValue(relationQuery.split(operator)[1].trim());
					restrictcond.setOperator(operator);
					restrictions.add(restrictcond);
					break;
				}
			}
		}
		
		if(restrictions.size()>1)
			this.logicalOperatorStore(conditionQuery);
	}
	
	private void logicalOperatorStore(String conditionQuery)
	{
		String conditionQueryData[]=conditionQuery.split(" ");
		
		for(String queryData:conditionQueryData)
		{
			queryData=queryData.trim();
			if(queryData.equals("and")||queryData.equals("or"))
			{
				logicalOperator.add(queryData);
			}
		}
	}
	
	

	private void fieldsProcessing(String selectColumn)
	{
		
		if(selectColumn.trim().contains("*") && selectColumn.trim().length()==1)
		{
			hasAllColumn=true;
		}
		else
		{
			String columnList[]=selectColumn.trim().split(",");
			
			for(String column:columnList)
			{
				columNames.getColumns().add(column.trim().toLowerCase());
			}
			
			if(selectColumn.contains("sum(") || selectColumn.contains("count(")||selectColumn.contains("count(*)"))
			{
				hasAggregate=true;
				hasAllColumn=true;
			}
		}
	}
	
	public RowHeader setHeaderRow() throws Exception
	{
		BufferedReader bufferedReader=new BufferedReader(new FileReader(getFilePath()));
		RowHeader headerRow=new RowHeader();
		
		if(bufferedReader!=null)
		{
			String rowData=bufferedReader.readLine();
			String rowValues[]=rowData.split(",");
			int columnIndex=0;
			for(String rowvalue:rowValues)
			{
				headerRow.put(rowvalue.toLowerCase(),columnIndex);
				columnIndex++;
			}
		}
		
		return headerRow;
	}
	
	public String getFilePath() {
		return filePath;
	}

	public String getGroupByColumn() {
		return groupByColumn;
	}

	public boolean isHasGroupBy() {
		return hasGroupBy;
	}

	public boolean isHasOrderBy() {
		return hasOrderBy;
	}

	public boolean isHasWhere() {
		return hasWhere;
	}

	public boolean isHasAllColumn() {
		return hasAllColumn;
	}

	public boolean isHasColumn() {
		return hasColumn;
	}

	public boolean isHasSimpleQuery() {
		return hasSimpleQuery;
	}

	public boolean isHasAggregate() {
		return hasAggregate;
	}
	
	public RowHeader getHeaderRow() 
	{
		return headerRow;
	}

	public List<String> getLogicalOperator() {
		return logicalOperator;
	}

	public SelectColumns getColumNames() {
		return columNames;
	}

	public void setColumNames(SelectColumns columnNames) {
		this.columNames = columnNames;
	}

	public List<RestrictionCondition> getListrelexpr() {
		return restrictions;
	}

	public void setListrelexpr(List<RestrictionCondition> listrelexpr) {
		this.restrictions = listrelexpr;
	}

	public String getOrderByColumn() 
	{
		return orderByColumn;
	}

}
