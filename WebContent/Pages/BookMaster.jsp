<%@ page import="java.sql.*"%>

<%
	ResultSet rsData = null;
	if(request.getParameter("BookId")!=null)
	{
		//Edit
		int iBookId = Integer.parseInt(request.getParameter("BookId"));
		System.out.println("Edit Book............."+iBookId);
		bms.common.BMSDBManager obj = new bms.common.BMSDBManager();	
		String sqlSel = "SELECT bookId,book_Name,author_name,book_price,book_description FROM mst_book Where bookId="+iBookId;
		rsData = obj.getResultSet(sqlSel);		
	}
%>

<HTML>
	<HEAD>
		<!-- <script type="text/javascript" src="Validate.js"></script -->>

	<TITLE>Book Master</TITLE>
	</HEAD>
<BODY>

<FORM action="BookOperation.jsp" method="post" onSubmit="return getValidate(this)">
<%
if(request.getParameter("BookId")==null)
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
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 175px; POSITION: absolute; TOP: 145px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Author Name &nbsp;<font color="red">*&nbsp;</font></B>
      	</FONT>
      </DIV>
    </TD>
    <TD>
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 145px" name="txtAuthorName">
    </TD>
  </TR>
  
  <TR>
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 175px; POSITION: absolute; TOP: 175px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Price&nbsp;<font color="red">*&nbsp;</font></B>
      	</FONT>
      </DIV>
    </TD>
    <TD>
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 175px" name="txtBookPrice">
    </TD>
  </TR>
  
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 200px; POSITION: absolute; TOP: 205px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Book Description</B>
      	</FONT> 
      </DIV>
    </TD>
    <TD>
    	<TEXTAREA style="LEFT: 340px; WIDTH: 330px; POSITION: absolute; TOP: 205px" name="txtBookDescription"></TEXTAREA> 
    </TD>
   </TR>
   
   <TR>
		<TD><input style="LEFT: 270px; OVERFLOW: hidden; WIDTH: 100px; POSITION: absolute; TOP: 350px; HEIGHT: 24px"
			type="Submit" name="btnSave" value="Save">
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

 <input type="hidden" name="BookId" value="<%=request.getParameter("BookId")%>">
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
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 175px; POSITION: absolute; TOP: 105px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Book Name &nbsp;<font color="red">*&nbsp;</font></B>
      	</FONT>
      </DIV>
    </TD>
    <TD>
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 105px" name="txtBookName" value="<%=rsData.getString("book_name")%>">
    </TD>
  </TR>
  
  <TR>
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 175px; POSITION: absolute; TOP: 135px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Author Name &nbsp;<font color="red">*&nbsp;</font></B>
      	</FONT>
      </DIV>
    </TD>
    <TD>
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 135px" name="txtAuthorName" value="<%=rsData.getString("author_name")%>">
    </TD>
  </TR>
  
  <TR>
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 175px; POSITION: absolute; TOP: 165px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Price&nbsp;<font color="red">*&nbsp;</font></B>
      	</FONT>
      </DIV>
    </TD>
    <TD>
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 165px" name="txtBookPrice" value="<%=rsData.getString("book_price")%>">
    </TD>
  </TR>
  
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 200px; POSITION: absolute; TOP: 195px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Book Description</B>
      	</FONT> 
      </DIV>
    </TD>
    <TD>
    	<TEXTAREA style="LEFT: 340px; WIDTH: 330px; POSITION: absolute; TOP: 195px" name="txtBookDescription"><%=rsData.getString("book_description")%></TEXTAREA> 
    </TD>
   </TR>
   
    
   	<TR>
	<TD><input style="LEFT: 270px; OVERFLOW: hidden; WIDTH: 100px; POSITION: absolute; TOP: 350px; HEIGHT: 24px"
			type="Submit" name="btnSave" value="Save">
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
			out.println("No Record To Edit");
		}
	}
%>



</FORM>
</BODY>
</HTML>
