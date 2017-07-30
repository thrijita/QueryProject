package com.niit.testcases;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.niit.csvparse.*;

public class TestCase 
{
	
	static QueryParameter queryParam;
	
	@BeforeClass
	public static void initialize()throws Exception
	{
		String query="select Name,Salary from E:\\Emp.csv where Name='Rakesh' and Salary>=20000 or City=Kolkata";
		queryParam=new QueryParameter(query);
		display();
	}
	
	@Test
	public void filePathCheck() 
	{
		assertEquals("File Path is","E:\\Emp.csv",queryParam.getFilePath());
	}
	
	@Test
	public void whereConditionCheck() 
	{
		assertEquals("Where Condition","empname",queryParam.getListrelexpr().get(0).getColumn());
	}
	
	@Test
	public void orderByColumnCheck()
	{
		assertEquals("Order By Column","city",queryParam.getOrderByColumn());
	}
	
	@Test
	public void columnNamesCheck()
	{
		assertEquals("Column Names","empname",queryParam.getColumNames().getColumns().get(0));
		assertEquals("Column Names","empsal",queryParam.getColumNames().getColumns().get(1));
	}
	
	@Test
	public void groupByColumnCheck()
	{
		assertEquals("Group By Column","city",queryParam.getGroupByColumn());
	}
	
	@Test
	public void logicalOperatorCheck()
	{
		assertEquals("Logical Operator","and",queryParam.getLogicalOperator().get(0));
	}
	
	public static void display()
	{
		System.out.print("Select Column List:");
		if(!queryParam.isHasAllColumn())
		{
			for(String column:queryParam.getColumNames().getColumns())
			{
			System.out.print(column+",");
			}
		}
		else
		{
			System.out.print("*");
		}
		
		System.out.println("\nFile Name : "+queryParam.getFilePath());
		
		
		
		if(queryParam.isHasWhere())
		{
			System.out.println("Where Condition List:");
			
			for(RestrictionCondition condition:queryParam.getListrelexpr())
			{
				System.out.println("Column Value:"+condition.getColumn()+" Operator:"+condition.getOperator()+" Value:"+condition.getValue());
			}
			
			if(queryParam.getLogicalOperator().size()>0)
			{
				System.out.print("Logical Operator:");	
				for(String logicalOperator:queryParam.getLogicalOperator())
				{
					System.out.print(logicalOperator+",");
				}
			}
		}
		
		if(queryParam.isHasGroupBy())
		{
			System.out.println("\nGroup By Column:"+queryParam.getGroupByColumn());
		}
		
		if(queryParam.isHasOrderBy())
		{
			System.out.println("\nOrder By Column:"+queryParam.getOrderByColumn());
		}
	}

}
