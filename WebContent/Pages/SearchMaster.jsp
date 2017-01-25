<%@ page import="java.sql.*"%>

<%
	ResultSet rsData = null;
	if(request.getParameter("txtBookName")!=null)
	{
		//Edit
		String iBookName = request.getParameter("txtBookName");
		System.out.println("Edit Book............."+iBookName);
		bms.common.BMSDBManager obj = new bms.common.BMSDBManager();	
		String sqlSel = "SELECT bookId,book_name,author_name,book_price,book_description FROM mst_book Where book_name="+iBookName;
		rsData = obj.getResultSet(sqlSel);		
	}
%>

<HTML>
	<HEAD>
		<script language="javascript">
			function FillCheckValue(chkObj,hiddObj)
			{
				if(chkObj.checked==true)
					hiddObj.value="1";
				else
					hiddObj.value="0";
			}
		</script>
		<script type="text/javascript" src="Validate.js"></script>

	<TITLE>Search Master</TITLE>
	</HEAD>
<BODY>

<FORM action="SearchList.jsp" method="post" onSubmit="return getValidate(this)">
<%
if(request.getParameter("txtBookName")==null)
{
%>

<TABLE>
  <TBODY>  
  <TR>
    <TD>
      <DIV style="LEFT: 480px; OVERFLOW: hidden; WIDTH: 336px; POSITION: absolute; TOP: 12px; HEIGHT: 20px">
     	<B >&nbsp;<font color="red">*&nbsp; Indicates Mandatory</font></B>
      </DIV>
	  
    </TD>
  </TR>  
  <TR>
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 175px; POSITION: absolute; TOP: 115px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Book Name &nbsp;<font color="red">*&nbsp;</font></B>
      	</FONT>
      </DIV>
    </TD>
    <TD>
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 115px" name="txtBookName">
    </TD>
  </TR>
  
  <TR>
    
   <TR>
		<TD><input style="LEFT: 270px; OVERFLOW: hidden; WIDTH: 100px; POSITION: absolute; TOP: 350px; HEIGHT: 24px"
			type="Submit" name="btnSearch" value="Search">
		</TD>
		<TD><input style="LEFT: 400px; OVERFLOW: hidden; WIDTH: 100px; POSITION: absolute; TOP: 350px; HEIGHT: 24px"
			type="button" name="btnClose" value="Close" onclick="javascript:history.back()">
		</TD>
	</TR>
 </TBODY>
</TABLE>

 <% 
	}
	else
	{
		if(rsData.next())
		{
%>

 <input type="hidden" name="BookName" value="<%=request.getParameter("BookName")%>">
<TABLE>
  <TBODY>  
  <TR>
    <TD>
      <DIV style="LEFT: 480px; OVERFLOW: hidden; WIDTH: 336px; POSITION: absolute; TOP: 12px; HEIGHT: 20px">
     	<B >&nbsp;<font color="red">*&nbsp; Indicates Mandatory</font></B>
      </DIV>
	  
    </TD>
  </TR>  
  <TR>
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 175px; POSITION: absolute; TOP: 115px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Book Name &nbsp;<font color="red">*&nbsp;</font></B>
      	</FONT>
      </DIV>
    </TD>
    <TD>
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 115px" name="txtBookName">
    </TD>
  </TR>
  
  <TR>
    
   <TR>
		<TD><input style="LEFT: 270px; OVERFLOW: hidden; WIDTH: 100px; POSITION: absolute; TOP: 350px; HEIGHT: 24px"
			type="Submit" name="btnSearch" value="Search">
		</TD>
		
	</TR>
 </TBODY>
</TABLE>
<%	
		}
		else
		{
			out.println("No Record To Edit");
		}
	}
%>



</FORM>
</BODY>
</HTML>
