<% 
	if(session.getAttribute("UserID")==null)
	{
		response.sendRedirect("Logout.jsp");
	}
	else
	{
%>
<HTML>
<HEAD><TITLE> Login Admin Page</TITLE>
</HEAD>

<Frameset rows ="20%,80%" FRAMEBORDER=0 FRAMESPACING=0>
<FRAME src = "Header.jsp" NAME="fraTop" scrolling="NO">

<FRAMESET cols="17%, 83%" FRAMEBORDER=0 FRAMESPACING=0>
<FRAME src = "MenuAdmin.jsp" Name = "fraLeft" scrolling="NO">
<FRAME src = "Blank1.jsp" Name = "fraRight" scrolling="YES">
</FRAMESET>
</FRAMESET>

</HTML>
<%
	}
%>