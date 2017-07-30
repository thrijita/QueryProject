package com.niit.testcases;

import static org.junit.Assert.assertNotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.niit.csvparse.*;

public class QueryProcessTestCase 
{
	static QueryValidator query;

	@BeforeClass
	public static void initialize()
	{
		query=new QueryValidator();
	}
	
	@Test
	public void allColumnsWithoutWhereClause()throws Exception
	{
		String queryString="select * from E:\\Emp.csv";
		assertNotNull(query.executeQuery(queryString));
		System.out.println(queryString);
		displayRecords(query.executeQuery(queryString));
	}
	
	@Test
	public void selectedColumnsWithoutWhereClause()throws Exception
	{
		String queryString="select Name,EmpID,Salary from E:\\Emp.csv";
		assertNotNull(query.executeQuery(queryString));
		System.out.println(queryString);
		displayRecords(query.executeQuery(queryString));
	}
	
	@Test
	public void allColumnsWithWhereClauseWithString()throws Exception
	{
		String queryString="select * from E:\\Emp.csv where Name=Will";
		DataSetter dataSet=query.executeQuery(queryString);
		assertNotNull(dataSet.getResultSet().size());
		System.out.println(queryString);
		displayRecords(dataSet);
	}
	@Test
	public void allColumnsWithWhereClauseWithInteger()throws Exception
	{
		String queryString1="select * from E:\\Emp.csv where Salary>=28000";
		DataSetter dataSet1=query.executeQuery(queryString1);
		assertNotNull(dataSet1.getResultSet().get(0));
		System.out.println(queryString1);
		displayRecords(dataSet1);
	}
	@Test
	public void allColumnsWithMultipleWhereClauseWithString()throws Exception
	{
		String queryString2="select * from E:\\Emp.csv where Salary>30000 or City = Bangalore and Name=Anant";
		DataSetter dataSet2=query.executeQuery(queryString2);
		assertNotNull(dataSet2.getResultSet().get(0));
		System.out.println(queryString2);
		displayRecords(dataSet2);
	}
	
	@Test
	public void allColumnsWithOrderByClause()throws Exception
	{
		String queryString2="select * from E:\\Emp.csv order by Salary";
		DataSetter dataSet2=query.executeQuery(queryString2);
		assertNotNull(dataSet2.getResultSet().get(0));
		System.out.println(queryString2);
		displayRecords(dataSet2);
	}
	
	@Test
	public void aggregateDataDisplay()throws Exception
	{
		String queryString3="select sum(Salary),min(Salary),max(Salary),count(City) from E:\\Emp.csv";
		DataSetter dataSet3=query.executeQuery(queryString3);
		assertNotNull(dataSet3.getAggregateRow());
		System.out.println(queryString3);
		displayRecords(dataSet3);
	}
	
	@Test
	public void aggregateDataDisplayWithWhereClause()throws Exception
	{
		String queryString4="select sum(Salary),min(Salary),max(Salary),count(City) from E:\\Emp.csv where City=Bangalore";
		DataSetter dataSet4=query.executeQuery(queryString4);
		assertNotNull(dataSet4.getAggregateRow());
		System.out.println(queryString4);
		displayRecords(dataSet4);
	}
	
	@Test
	public void groupByDataDisplayWithWhereClause()throws Exception
	{
		String queryString4="select Dept,sum(Salary),min(Salary),max(Salary),count(City) from E:\\Emp.csv where Dept=Sales or Dept=IT group by Dept";
		DataSetter dataSet4=query.executeQuery(queryString4);
		assertNotNull(dataSet4.getAggregateRow());
		System.out.println(queryString4);
		displayGroupByRecords(dataSet4);
	}
	
	public void displayGroupByRecords(DataSetter dataSet4)
	{
		LinkedHashMap<String,LinkedHashMap<String,Float>> groupRows=dataSet4.getTotalGroupedData();
		
		Set<String> groupByColumnValues=groupRows.keySet();
		
		for(String groupByColumnValue:groupByColumnValues)
		{
			LinkedHashMap<String,Float> eachGroupRow=dataSet4.getTotalGroupedData().get(groupByColumnValue);
			System.out.print(groupByColumnValue+"\t");
			Set<String> aggregateColumnNames=eachGroupRow.keySet();
			for(String eachAggregateColumnName:aggregateColumnNames)
			{
				System.out.print(eachGroupRow.get(eachAggregateColumnName)+"\t");
			}
			
			System.out.println();
		}
		
		
	}
	public void displayRecords(DataSetter dataSet)
	{
		if(dataSet.getAggregateRow().isEmpty())
		{
		
			for(RowData rowData:dataSet.getResultSet())
			{
				Set<Integer> rowIndex=rowData.keySet();
				
				for(int index:rowIndex)
				{
					System.out.print(rowData.get(index)+"\t");
				}
				
				System.out.println();
			}
		
		}
		else
		{
			System.out.println();
			Set<String> columnNames=dataSet.getAggregateRow().keySet();
			
			for(String columnName:columnNames)
			{
				System.out.print(columnName+"\t");
			}
			System.out.println();
			for(String columnName:columnNames)
			{
				System.out.print(dataSet.getAggregateRow().get(columnName)+"\t\t");
			}
			System.out.println();
		}
	}

}
