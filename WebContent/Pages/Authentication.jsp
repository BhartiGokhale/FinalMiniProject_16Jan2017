<%@ page import="java.sql.*"%>
<html>
<head>
	<title>Authentication</title>

</head>
<body>


<%
	bms.common.BMSDBManager obj = new bms.common.BMSDBManager();
	try
	{
		String name = request.getParameter("txtUserName");
		String pass = request.getParameter("txtUserPassword");
		ResultSet authent= null;
		String sql = "Select * from mst_user  WHERE "+ 
					 "user_name ='"+name+"' and user_password = '"+pass+"'";
		System.out.println(sql);
		authent = obj.getResultSet(sql);
	if(authent.next())
	{
		String str1 =  authent.getString("role");
	
		if(str1.equals("Admin"))
		{	
			System.out.println("HI.....................");		
			System.out.println(str1);
			System.out.println("Designation ID :" +str1);
			session.setAttribute("UserName",authent.getString("First_Name"));
			session.setAttribute("UserID",authent.getString("UserId"));
			
			response.sendRedirect("LoginAdmin.jsp");
			
		}
		else
		{	
			System.out.println("HI.....................");		
			System.out.println("Role ID :" +str1);
			session.setAttribute("UserName",authent.getString("First_Name"));
			session.setAttribute("UserID",authent.getString("UserId"));
			response.sendRedirect("LoginUser.jsp");
		}
	}
	
	else
	{
		System.out.println("Invalid UserName or Password");
	 	response.sendRedirect("InvalidLogin.jsp");
	}
}
	catch(Exception e)
	{
		response.sendRedirect("InvalidLogin.jsp");	
	}
%>

</body>
</html>
