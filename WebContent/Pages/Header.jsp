<%@ page import="java.sql.*"%>

<%
	bms.common.BMSDBManager obj = new bms.common.BMSDBManager();	
%>
<HTML>
<head>
<title> Header </title>
<STYLE type=text/css>
	A.style2:visited {
		COLOR: #666699 ; TEXT-DECORATION: none
	}
	A.style2:active {
		TEXT-DECORATION: none
	}
	A.style2:hover {
		BACKGROUND: white; COLOR:red ; TEXT-DECORATION: none
	}
</STYLE>

<body >


<form action = "" name="formlog" method = "post">
		<TR>
		  <TD>
      		  <DIV style="LEFT: 154px; OVERFLOW: hidden; WIDTH: 756px; POSITION: absolute; TOP: 22px; HEIGHT: 79px">
				<FONT size=3 face=Tahoma color=#808080><B><marquee>Welcome <%=session.getAttribute("UserName")%></marquee><br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; </B></FONT>
				<FONT size=8 face="Arial Rounded MT Bold" color=#808080><B>Book Review Management System</B></FONT>     		  
       		  </DIV>
       		  <DIV style="LEFT: 954px; OVERFLOW: hidden; WIDTH: 756px; POSITION: absolute; TOP: 82px; HEIGHT: 79px">
				<FONT size=2 face="Arial Rounded MT Bold" color=#808080><a href="Logout.jsp" target="_top">Logout</a></FONT>     		  
       		  </DIV>
       		  
       		  
    		  </TD>
   		 </TR>
</FORM>
</BODY>
</HTML>