<%@ page import="java.util.*"%>
<%
	String name=(String)session.getAttribute("UserName");
	String userid=(String)session.getAttribute("UserID");
	int strBookId = Integer.parseInt(request.getParameter("BookId")!=null?(request.getParameter("BookId")):"");
	String strBookName = (request.getParameter("txtBookName")!=null?request.getParameter("txtBookName"):"");
	String strAuthorName = (request.getParameter("txtAuthorName")!=null?request.getParameter("txtAuthorName"):"");
	String strPrice = (request.getParameter("txtBookPrice")!=null?request.getParameter("txtBookPrice"):"");
	String strBookDescription = (request.getParameter("txtBookDescription")!=null?request.getParameter("txtBookDescription"):"");
	String strComment = (request.getParameter("txtBookReview")!=null?request.getParameter("txtBookReview"):"");
	
	
	Calendar cal = Calendar.getInstance();
  	String strCurrDate = cal.get(Calendar.YEAR)  + "-"+ (cal.get(Calendar.MONTH) +1) +"-" + cal.get(Calendar.DATE) ;
	
  	bms.common.BMSDBManager obj = new bms.common.BMSDBManager();
	String sqlIns = "";
	if(request.getParameter("BookId")!=null)						//ADD NEW
	{
		int PKID = obj.getPKID("mst_book_review","reviewid");
		sqlIns = "Insert into mst_book_review(reviewid,bookId,author_name,reviewedby,revieweddate,comment,deleteflag,enteredby,entereddate,updatedby,updateddate";
		sqlIns += ") Values("+PKID+","+strBookId+",'"+strAuthorName.trim()+"','"+userid+"','"+strCurrDate+"', '"+strComment+"','N',";
		sqlIns += " '"+userid+"','"+strCurrDate+"','"+userid+"','"+strCurrDate+"')";
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
		response.sendRedirect("ReviewList.jsp?BookId="+strBookId+"");
%>