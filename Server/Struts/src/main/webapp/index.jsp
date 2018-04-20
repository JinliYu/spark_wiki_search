<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="s" uri="/struts-tags"%>
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8"/>
    <title>Search</title>
    <!-- <link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet"> -->
    <link rel="stylesheet" type="text/css" href="bootstrap/css/bootstrap.css" />
    <link rel="stylesheet" type="text/css" href="css/search.css" />


</head>

<body>
    


<div id="content" role="main">

    
    <div class="container">

        <div class="row">
           <div class="nav">
                
                

           </div>
           <div class="logo col-md-4 col-md-offset-4 text-center">Wikipedia</div>
           <div class="searchArea col-md-12">
                <form action="search" method="post">
                   <div class="searchInput input-group col-md-8 col-md-offset-2 form-group">
                       <input type="text" class="form-control" name = "keywords" placeholder="please input keywords. For multiple keywords, please use 'and', 'or', 'not' and '( )'" aria-describedby="basic-addon2" id="search">
                        <span class="input-group-addon"><input type="submit" class="btn btn-primary btn-block" id="submit-button" value="search"></span>

                   </div>
                    
                 </form>
                    <!-- <div class="power-search col-md-2 col-md-offset-5">
                        <a href="/LibraryManagementSys/WebPage/power-search/psearch.jsp"><button class="btn btn-primary btn-block" id='power-search-button'>Search</button></a>
                    </div> -->
            </div>
        </div>
    </div>

</div> <!-- close #content -->


    <script src="js/jquery.js"></script>
    <!-- <script src="//api.filepicker.io/v2/filepicker.js"></script> -->
    <script src="bootstrap/js/bootstrap.js"></script>
    <script src="js/common.js"></script>
    <script src="js/app.js"></script>

</body>
</html>
