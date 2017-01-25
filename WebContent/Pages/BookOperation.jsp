<%@ page import="java.util.*"%>
<%
	String name=(String)session.getAttribute("UserName");
	String userid=(String)session.getAttribute("UserID");

	String strBookName = (request.getParameter("txtBookName")!=null?request.getParameter("txtBookName"):"");
	String strAuthorName = (request.getParameter("txtAuthorName")!=null?request.getParameter("txtAuthorName"):"");
	String strPrice = (request.getParameter("txtBookPrice")!=null?request.getParameter("txtBookPrice"):"");
	String strBookDescription = (request.getParameter("txtBookDescription")!=null?request.getParameter("txtBookDescription"):"");
	
	
	Calendar cal = Calendar.getInstance();
  	String strCurrDate = cal.get(Calendar.YEAR)  + "-"+ (cal.get(Calendar.MONTH) +1) +"-" + cal.get(Calendar.DATE) ;
	
  	bms.common.BMSDBManager obj = new bms.common.BMSDBManager();
	String sqlIns = "";
	if(request.getParameter("BookId")==null)						//ADD NEW
	{
		int PKID = obj.getPKID("mst_book","bookid");
		sqlIns = "Insert into mst_book(bookId,book_Name,author_name,book_price,book_Description,DeleteFlag,";
		sqlIns += "EnteredBy,EnteredDate) Values("+PKID+",'"+strBookName.trim()+"','"+strAuthorName.trim()+"','"+strPrice.trim()+"',";
		sqlIns += " '"+strBookDescription.trim()+"','N','"+userid+"','"+strCurrDate+"')";
	}
	else
	{
		int iBookId = Integer.parseInt(request.getParameter("BookId"));
		if(request.getParameter("DeleteFlag")!=null)										//DELETE
		{
			sqlIns = " Update mst_book Set DeleteFlag = 'Y' WHERE bookId = "+iBookId;
		}
		else																//EDIT
		{			
			sqlIns = " Update mst_book Set book_name = '"+strBookName+"',author_name = '"+strAuthorName.trim()+"', book_price = '"+strPrice.trim()+"',"+
					 " book_description= '"+strBookDescription.trim()+"', UpdatedBy =1, "+						
					 " UpdatedDate = '"+strCurrDate+"' WHERE bookId = "+iBookId;		
			
		}
	}		  
	boolean booResult = obj.executeQuery(sqlIns);
	
	System.out.println("Check2 "+booResult);
	System.out.println("Query "+sqlIns);
	
	if(booResult)
		response.sendRedirect("BookList.jsp");
%>