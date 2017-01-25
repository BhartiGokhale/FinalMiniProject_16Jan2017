<%@ page import="java.sql.*"%>
<%@ page import="java.util.*"%>

<%		
	bms.common.BMSDBManager obj = new bms.common.BMSDBManager();		
	String sqlSel = "Select bookId,book_Name,author_name,book_price,book_Description from mst_book WHERE DeleteFlag = 'N' order by bookId ";	
	ResultSet rsBook  = obj.getResultSet(sqlSel);		
%>

<html>
<head>
<script language="javascript">
	function Delete()
	{
		DeleteFlag = confirm("Do you really want to delete this Record ?");
	    if (DeleteFlag==true)
	      return true;
	    else
	      return false;
	}

</script>

<script type="text/javascript" src="sortable.js"></script>

	<style type="text/css">
	            .pg-normal {
	                color: black;
	                font-weight: normal;
	                text-decoration: none;
	                cursor: pointer;
	            }
	            .pg-selected {
	                color: black;
	                font-weight: bold;
	                text-decoration: underline;
	                cursor: pointer;
	            }
	  </style>

     <script type="text/javascript" src="paging.js"></script>
     <link rel="stylesheet" type="text/css" href="example.css"/>

</head>

<body bgcolor="EFF7FF">
<Form name="myGridForm">
<div style="width:100%;Height=100%">
	<center>
		<a href="BookMaster.jsp">AddNew</a>
		<a href="Blank1.jsp">Close</a>
	</center>	
	<table  name="tblGrid"  cellpadding="0" cellspacing="0" id="results" align='center' height="100%" width="100%">

		<TR bgcolor="#00FFCC">	
			<TD colspan='7' width="100%"><B>Book Information</B></TD>					
		</TR>
		
		<TR bgcolor="#00CCDD">	
			<TD width='10%'>Sr No</TD>	
			<TD width='20%'>Book Name</TD>
			<TD width='20%'>Author Name</TD>
			<TD width='20%'>Price</TD>	
			<TD width='30%'>Book Description</TD>	
			<TD>Edit</TD>
			<TD>Delete</TD>
		</TR>

		<%
			int sN=1;
			while(rsBook.next())
			{
				int iBookID = rsBook.getInt("bookId");
				
				if(sN%2==0)
				{
		%>
		
		<TR bgcolor="#DEEEFF">	
			<TD width='10%'><%=sN%></TD>			
			<TD width='20%'><%=rsBook.getString("book_Name")%></TD>
			<TD width='20%'><%=rsBook.getString("author_Name")%></TD>
			<TD width='20%'><%=rsBook.getString("book_price")%></TD>	
			<TD width='30%'><%=rsBook.getString("book_Description")%></TD>	
			<TD><a href="BookMaster.jsp?BookId=<%=iBookID%>">Edit</a>&nbsp;&nbsp;</TD>
			<TD><a onclick="return Delete();" href="BookOperation.jsp?BookId=<%=iBookID%>&DeleteFlag=true">Delete</a></TD>
		</TR>
		<% 
				}
				else
				{
		%>
	<TR bgcolor="#EFF7FF">	
			<TD width='10%'><%=sN%></TD>			
			<TD width='20%'><%=rsBook.getString("book_Name")%></TD>
			<TD width='20%'><%=rsBook.getString("author_Name")%></TD>
			<TD width='20%'><%=rsBook.getString("book_price")%></TD>	
			<TD width='30%'><%=rsBook.getString("book_Description")%></TD>	
			<TD><a href="BookMaster.jsp?BookId=<%=iBookID%>">Edit</a>&nbsp;&nbsp;</TD>
			<TD><a onclick="return Delete();" href="BookOperation.jsp?BookId=<%=iBookID%>&DeleteFlag=true">Delete</a></TD>
		</TR>
			
		<%		}
			sN++;
			}
		 %>
</table>
</div>
<BR><BR>
<div id="pageNavPosition" align='center'></div>

 <script type="text/javascript">
        var pager = new Pager('results', 5);
        pager.init();
        pager.showPageNav('pager', 'pageNavPosition');
        pager.showPage(1);
 </script>

</Form>
</body>
</html> 