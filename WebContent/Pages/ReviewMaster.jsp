<%@ page import="java.sql.*"%>

<%
	ResultSet rsData = null;
	if(request.getParameter("BookId")!=null)
	{
		//Edit
		int iBookId = Integer.parseInt(request.getParameter("BookId"));
		System.out.println("Edit Book............."+iBookId);
		bms.common.BMSDBManager obj = new bms.common.BMSDBManager();	
		String sqlSel = "SELECT book_Name,author_name,book_price,book_description FROM mst_book Where mst_book.bookId="+iBookId;
		rsData = obj.getResultSet(sqlSel);		
	}
%>

<HTML>
	<HEAD>
		<!-- <script language="javascript">
			function FillCheckValue(chkObj,hiddObj)
			{
				if(chkObj.checked==true)
					hiddObj.value="1";
				else
					hiddObj.value="0";
			}
		</script>
		<script type="text/javascript" src="Validate.js"></script> -->

	<TITLE>Review Master</TITLE>
	</HEAD>
<BODY>

<FORM action="ReviewOperation.jsp?BookId=<%=Integer.parseInt(request.getParameter("BookId"))%> " method="post" onSubmit="return getValidate(this)">
<%
if(request.getParameter("BookId")!=null)
{
		if(rsData.next())
		{
%>

 <input type="hidden" name="ReviewId" value="<%=request.getParameter("ReviewId")%>">
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
      <INPUT  mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 115px" name="txtBookName" value="<%=rsData.getString("book_name")%>" readonly>
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
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 145px" name="txtAuthorName" value="<%=rsData.getString("author_name")%>" readonly>
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
      <INPUT  mandatory= 'Y' mess='Book Name' onKeyPress="return AllowOnlyNumbersAndChars(event);" style="LEFT: 340px; WIDTH: 130px; POSITION: absolute; TOP: 175px" name="txtBookPrice" value="<%=rsData.getString("book_price")%>" readonly>
    </TD>
  </TR>
  <TR>
    <TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 200px; POSITION: absolute; TOP: 205px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Book Description</B>
      	</FONT> 
      </DIV>
    </TD>
    <TD>
    	<TEXTAREA style="LEFT: 340px; WIDTH: 330px; POSITION: absolute; TOP: 205px" name="txtBookDescription"><%=rsData.getString("book_description")%></TEXTAREA> 
    </TD>
   </TR>
   
 	<TR>
			<TD>
      <DIV style="LEFT: 130px; OVERFLOW: hidden; WIDTH: 200px; POSITION: absolute; TOP: 235px; HEIGHT: 20px">
      	<FONT face=Tahoma color=#808080><B>Add Review</B>
      	</FONT> 
      </DIV>
    </TD>
 <TD>
    	<TEXTAREA style="LEFT: 340px; WIDTH: 330px; POSITION: absolute; TOP: 235px" name="txtBookReview"></TEXTAREA> 
    </TD>
 	
   	<TR>
	<TD><input style="LEFT: 270px; OVERFLOW: hidden; WIDTH: 100px; POSITION: absolute; TOP: 350px; HEIGHT: 24px"
			type="Submit" name="btnAdd" value="Add">
			
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
