<%@ page import="java.sql.*"%>

<%
	bms.common.BMSDBManager obj = new bms.common.BMSDBManager();	
%>
<HTML>
<head>
<title> LOGIN </title>
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

<script language="javascript">
		function validateData()
		{
			if(document.formlog.txtUserName.value=='')
			{
				alert("Please Enter User Name");
				return false;
			}
			if(document.formlog.txtUserPassword.value=='')
			{
				alert("Please Enter Password");
				return false;
			}
		  return true;
		}

</script>
</head>

<body background="../Images/a-book-a-week-image.png" >


<form action = "Authentication.jsp" name="formlog" method = "post" >
<div align="center">
		 
		 				
		<TR>
		  <TD>
      		  <DIV style="LEFT: 20px; OVERFLOW: hidden; WIDTH: 800px; POSITION: absolute; TOP: 22px; HEIGHT: 79px">
				<FONT size=3 face=Tahoma color=#808080><B>Welcome To,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; </B></FONT>
				<FONT size=8 face="Arial Rounded MT Bold" color=#808080><B>Book Review Management System</B></FONT>
       		  </DIV>
    		  </TD>
   		 </TR>

		<TR>
    		  <TD>
			    <DIV  style="LEFT: 672px; OVERFLOW: hidden; WIDTH: 250px; POSITION: absolute; TOP: 205px; HEIGHT: 33px">
				<FONT size=2 color=#808080 face=Tahoma><B>Invalid Username or Password</B>
				</FONT>
				</DIV>
				
				<DIV align="center" style="LEFT: 672px; OVERFLOW: hidden; WIDTH: 150px; POSITION: absolute; TOP: 254px; HEIGHT: 33px">
				<FONT size=2 color=#808080 face=Tahoma><B>&nbsp;&nbsp;&nbsp;&nbsp;Username :</B>
				</FONT>
				</DIV>
				
				<div align="center">
				<td>
				<INPUT  style="LEFT: 792px; WIDTH: 166px; POSITION: absolute; TOP: 251px" name=txtUserName>	
				</INPUT>
				</td>
				</DIV>
		
				<td>
				<INPUT  type="password" style="LEFT: 792px; WIDTH: 166px; POSITION: absolute; TOP: 294px" name=txtUserPassword></INPUT>
				</td>
				
				<DIV align="center" style="LEFT: 672px; OVERFLOW: hidden; WIDTH: 150px; POSITION: absolute; TOP: 294px; HEIGHT: 33px">
				<FONT size=2 color=#808080 face=Tahoma><B>&nbsp;&nbsp;&nbsp;&nbsp;Password :</B>
				</FONT>
				</DIV>
      		  
      		  </DIV>
      		  
    		  </TD>
   		 </TR>
   		 
   		 <TR>
    		  <TD>
				<DIV align=center style="LEFT: 792px; OVERFLOW: hidden; WIDTH: 75px; POSITION: absolute; TOP: 363px; HEIGHT: 25px">
      			  <INPUT TYPE="SUBMIT" NAME="BTNLOGIN" VALUE="LOGIN" onClick="return validateData();" id='nm' STYLE="width:75;height:25">
      		  </DIV>
    		  </TD>
   		 </TR>
   		 		

		 <!--  <TR>
    		  <TD>
				<DIV align=center style="LEFT: 770px; OVERFLOW: hidden; WIDTH: 122px; POSITION: absolute; TOP: 405px; HEIGHT: 20px">			
      			  <FONT size=2 face=Tahoma color=#808080><B><a class="style2" href="Register.jsp" target="_top">Register</a></B>
      			  </FONT>
      		  </DIV>
    		  </TD>
   		 </TR> -->

	</div>
</FORM>

</BODY>
</HTML>