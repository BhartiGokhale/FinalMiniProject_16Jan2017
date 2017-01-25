package bms.common;

import java.sql.*;
import javax.sql.*;
import java.util.Date;
import java.util.logging.Logger;
import java.util.*;
import java.io.*;

public class BMSDBManager
{	static Logger logger = Logger.getLogger(BMSDBManager.class.getName());
	Connection con = null;
	
	public BMSDBManager()
	{
		
	}
	
	public void openConnection()
	{
			try
			{			
				String driverName="com.mysql.jdbc.Driver";
				logger.info("Loading Driver ......."+driverName);
				Class.forName(driverName);
				logger.info("Driver Loaded inside DBManager getConnection Method"+driverName);			
			}catch(ClassNotFoundException cnfe)
			{
				logger.info("Class Not Found Exception Has Occured");
			}
			try
			{
				String url="jdbc:mysql://localhost:3306/cybage";			
				con=DriverManager.getConnection(url,"root","root");
				logger.info("Conection Established inside OpenConnection"+url);
			}catch(SQLException sqle)
			{
				logger.info("SQL Exception Has Occured"+sqle);
			}
		}
	
	
		public Connection getConnection()
		{
			try
			{
				String driverName="com.mysql.jdbc.Driver";
				Class.forName(driverName);
			}catch(ClassNotFoundException cnfe)
			{
				logger.warning("Class Not Found Exception Has Occured");
			}
			try
			{
				String url="jdbc:mysql://localhost:3306/cybage";
				Connection con1=DriverManager.getConnection(url,"root","root");
				return con1;
			}catch(SQLException sqle)
			{
				logger.warning("SQL Exception Has Occured");
				return null;
			}
	
		}
	/******************************************************************************/
			// Method Which Closes the Connection
	/******************************************************************************/
		public void closeConnection()
		{
			try
			{
				if(con!=null)
				{
					con.close();
					con = null;
				}
				
			}catch(SQLException sql)
			{
				logger.warning("Error While Closing Connection");
			}
		}
		
	public int getPKID(String strTblName,String strFieldName)
	{
		if(con==null)
			openConnection();
		System.out.println("Inside getPKID");
		int id=0;
		try
		{
			String sql="select max(" + strFieldName + ") from "+strTblName;
			logger.info("Inside getPKID"+strTblName);
			ResultSet tmpRs=getResultSet(sql);
			if(tmpRs.next())
			{
				id=tmpRs.getInt(1);
				id=id+1;
				logger.info("Inside IF MAX ID ="+id);
			}
			else
			{
				id=1;
				logger.info("Inside ELSE MAX ID ="+id);
			}
		}catch(Exception e)
		{
			logger.warning("Exception Ocuured inside getPKID Function"+e);
		}

		return id;
	}
	/******************************************************************************/
			// Method Which is Used For Queries like Insert, Update, Delete
	/******************************************************************************/
		public boolean executeQuery(String SQ)
		{
			//System.out.println("Inside ExecuteQuery"+SQ);
			int rowsAffected=0;
			try
			{
				if(con==null)
					openConnection();
			//	System.out.println("Connection Est Inside ExecuteQuery"+con);
				Statement stat=con.createStatement();
				rowsAffected=stat.executeUpdate(SQ);
			//	System.out.println("Row Affected Inside ExecuteQuery"+SQ+".... ROW AFFCTED.........."+rowsAffected);
			}catch(SQLException sqle)
			{
				logger.warning("Exeption executeQuery ....."+sqle);
			}
	
	
				if(rowsAffected>=1)
					return true;
				else
					return false;
		}

	
/******************************************************************************/
		// Method Which is Used to Get Result Set   For Select Query
/******************************************************************************/
	public ResultSet getResultSet(String SQ)
	{
		ResultSet rs = null;
		try
		{
			if(con==null)
				openConnection();
			logger.info("Test1...con...."+con);
			Statement stat=con.createStatement();
			logger.info("Test2...."+SQ);
			rs=stat.executeQuery(SQ);
		}catch(SQLException sqle)
		{
			logger.warning("SQL Exception Inside getResultSet"+sqle);
		}

		return rs;
	}

}