<%@ page import="java.sql.*"%>
<%@ page import="java.util.*"%>

<%		
	bms.common.BMSDBManager obj = new bms.common.BMSDBManager();
	int iBookId = Integer.parseInt(request.getParameter("BookId"));
	String sqlSel = "Select mst_book.bookid,book_Name,user_name,first_name,last_name,mst_book_review.author_Name,reviewid,reviewedby,revieweddate,comment from mst_book,mst_book_review,mst_user WHERE mst_book.bookid=mst_book_review.bookid and mst_user.userid=mst_book_review.enteredby and mst_book.DeleteFlag = 'N' and mst_book.bookid="+iBookId;	
	ResultSet rsBook  = obj.getResultSet(sqlSel);		
%>

<html>
<head>


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
		<a href="SearchMaster.jsp">AddNew</a>
		<a href="Blank1.jsp">Close</a>
	</center>	
	<table  name="tblGrid"  cellpadding="0" cellspacing="0" id="results" align='center' height="100%" width="100%">

		<TR bgcolor="#00FFCC">	
			<TD colspan='7' width="100%"><B>Review Information</B></TD>					
		</TR>
		
		<TR bgcolor="#00CCDD">	
			<TD width='10%'>Sr No</TD>	
			<TD width='20%'>Book Name</TD>
			<TD width='10%'>Author Name</TD>
			<TD width='10%'>Reviewed by</TD>	
			<TD width='20%'>Review Date</TD>
			<TD width='30%'>Comment</TD>	
		</TR>

		<%
			int sN=1;
			while(rsBook.next())
			{
				int ireviewID = rsBook.getInt("reviewid");
				
				if(sN%2==0)
				{
		%>
		
		<TR bgcolor="#DEEEFF">	
			<TD width='10%'><%=sN%></TD>			
			<TD width='20%'><%=rsBook.getString("book_Name")%></TD>
			<TD width='20%'><%=rsBook.getString("author_Name")%></TD>
			<TD width='20%'><%=rsBook.getString("first_name")%></TD>
			<TD width='20%'><%=rsBook.getString("revieweddate")%></TD>	
			<TD width='30%'><%=rsBook.getString("comment")%></TD>	
<%-- 			<TD><a onclick="return Delete();" href="ReviewOperation.jsp?reviewid=<%=ireviewID%>&DeleteFlag=true">Delete</a></TD> --%>
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
			<TD width='20%'><%=rsBook.getString("first_name")%></TD>
			<TD width='20%'><%=rsBook.getString("revieweddate")%></TD>
			<TD width='30%'><%=rsBook.getString("comment")%></TD>	
<%-- 			<TD><a onclick="return Delete();" href="ReviewOperation.jsp?reviewid=<%=ireviewID%>&DeleteFlag=true">Delete</a></TD> --%>
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