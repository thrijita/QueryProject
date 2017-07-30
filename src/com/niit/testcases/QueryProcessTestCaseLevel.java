package com.niit.testcases;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.niit.csvparse.*;


public class QueryProcessTestCaseLevel 
{
	static QueryValidator query;

	@BeforeClass
	public static void initialize()
	{
		query=new QueryValidator();
	}

	@Test
	public void selectAllWithoutWhereTestCase()throws Exception //Test Case 1
	{
		
		DataSetter dataSet=query.executeQuery("select * from E:/Emp.csv");
		assertNotNull(dataSet);
		display("selectAllWithoutWhereTestCase",dataSet);
		
	}
	
	@Test
	public void selectColumnsWithoutWhereTestCase()throws Exception //Test Case 2
	{
		
		DataSetter dataSet=query.executeQuery("select City,Dept,Name from E:/Emp.csv");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereGreaterThanTestCase()throws Exception //Test Case 3
	{
		
		DataSetter dataSet=query.executeQuery("select City,Name,Salary from E:/Emp.csv where salary>30000");
		assertNotNull(dataSet);
		display("withWhereGreaterThanTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereLessThanTestCase()throws Exception //Test Case 4
	{
		
		DataSetter dataSet=query.executeQuery("select City,Name,Salary from E:/Emp.csv where Salary<35000");
		assertNotNull(dataSet);
		display("withWhereLessThanTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereLessThanOrEqualToTestCase()throws Exception //Test Case 5
	{
		
		DataSetter dataSet=query.executeQuery("select City,Name,Salary from E:/Emp.csv where Salary<=35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereGreaterThanOrEqualToTestCase()throws Exception //Test Case 6
	{
		
		DataSetter dataSet=query.executeQuery("select city,name,salary from E:/Emp.csv where Salary>=35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereNotEqualToTestCase()throws Exception //Test Case 7
	{
		
		DataSetter dataSet=query.executeQuery("select City,Name,Salary from E:/Emp.csv where Salary>=35000");
		assertNotNull(dataSet);
		display("withWhereNotEqualToTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereEqualAndNotEqualTestCase()throws Exception //Test Case 8
	{
		
		DataSetter dataSet=query.executeQuery("select City,Name,Salary from E:/Emp.csv where Salary>=30000 and salary<=38000");
		assertNotNull(dataSet);
		display("withWhereEqualAndNotEqualTestCase",dataSet);
		
	}
	
	@Test
	public void selectCountColumnsWithoutWhereTestCase()throws Exception //Test Case 9
	{
		
		DataSetter dataSet=query.executeQuery("select count(Name) from E:/Emp.csv");
		assertNotNull(dataSet);
		display("selectCountColumnsWithoutWhereTestCase",dataSet);
		
	}
	
	@Test
	public void selectSumColumnsWithoutWhereTestCase()throws Exception //Test Case 10
	{
		
		DataSetter dataSet=query.executeQuery("select sum(Salary) from E:/Emp.csv");
		assertNotNull(dataSet);
		display("selectSumColumnsWithoutWhereTestCase",dataSet);
		
	}
	
	@Test
	public void selectSumColumnsWithWhereTestCase()throws Exception //Test Case 11
	{
		
		DataSetter dataSet=query.executeQuery("select sum(Salary) from E:/Emp.csv where city=Bangalore");
		assertNotNull(dataSet);
		display("selectSumColumnsWithWhereTestCase",dataSet);
		
	}
	
	@Test
	public void selectColumnsWithoutWhereWithOrderByTestCase()throws Exception //Test Case 12
	{
		
		DataSetter dataSet=query.executeQuery("select City,Name,Salary from E:/Emp.csv order by Salary");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase",dataSet);
		
	}
	
	@Test
	public void selectColumnsWithWhereWithOrderByTestCase()throws Exception //Test Case 13
	{
		
		DataSetter dataSet=query.executeQuery("select City,Name,Salary from E:/Emp.csv where City=Bangalore order by Salary");
		assertNotNull(dataSet);
		display("selectColumnsWith-WhereWithOrderByTestCase",dataSet);
		
	}
	
	@Test
	public void selectColumnsWithoutWhereWithGroupByCountTestCase()throws Exception //Test Case 14
	{
		
		DataSetter dataSet=query.executeQuery("select City,count(*) from E:/Emp.csv group by City");
		assertNotNull(dataSet);
		displayGroupByRecords("selectColumnsWithoutWhereWithOrderByTestCase",dataSet);
		
	}
	
	@Test 
	public void selectColumnsWithoutWhereWithGroupBySumTestCase()throws Exception //Test Case 15
	{
		
		DataSetter dataSet=query.executeQuery("select City,sum(Salary) from E:/Emp.csv group by City");
		assertNotNull(dataSet);
		displayGroupByRecords("selectColumnsWithoutWhereWithOrderByTestCase",dataSet);
		
	}
	
	public void display(String str,DataSetter dataSet)
	{
		System.out.println();
		System.out.println(str);
		System.out.println();
		
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
				System.out.print(dataSet.getAggregateRow().get(columnName)+"\t");
			}
		}
	}
	
	
	public void displayGroupByRecords(String str,DataSetter dataSet4)
	{
		System.out.println();
		System.out.println(str);
		System.out.println();
		
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

}
