function fetch_elastic_data(){
  // Connect to the public API.
  	
  //URL Endpoint Elasticsearch
  var url_cell='B3';
  var url= SpreadsheetApp.getActiveSheet().getRange(url_cell).getValue();
  
  //User elasticsearch
  var user_cell='B4';
  var user= SpreadsheetApp.getActiveSheet().getRange(user_cell).getValue();
  
  //Psswd elasticsearch
  var psswd_cell='B5';
  var psswd=SpreadsheetApp.getActiveSheet().getRange(psswd_cell).getValue();
  
  //index
  var index_cell='B6';
  var index=SpreadsheetApp.getActiveSheet().getRange(index_cell).getValue();
  
  //max docs
  var ndocs_cell='B7';
  var ndocs=SpreadsheetApp.getActiveSheet().getRange(ndocs_cell).getValue();
  
//get spreadsheet
var ss = SpreadsheetApp.getActiveSpreadsheet();
var sheet = ss.getActiveSheet();

  
  //basic auth params
  var basic_auth=user.concat(':',psswd);
  
//"terms" query parameters
var q_range_1='E4';
var q_range_2='F6';
  var q_range= q_range_1.concat(':',q_range_2)
var range = sheet.getRange(q_range);
var tvalues = range.getValues(); //values for "terms" query

//parameteters for range query
var q_range_1='H4';
var q_range_2='J7';
  var q_range= q_range_1.concat(':',q_range_2)
var range = sheet.getRange(q_range);
var fvalues = range.getValues(); //values for terms query
  
  
  var terms=[];
  var filters=[];
  
  
  var query={};


  //queries terms
  for (var i = 0; i < tvalues.length; i++) {
      if (tvalues[i][0].length>0 & JSON.stringify(tvalues[i][1]).length>0){
        
        if (typeof tvalues[i][1]=== "boolean"){
  // variable is a boolean
          // (TRUE,FALSE)
          //match query
          var terms_temp={"match":{}};
                terms_temp["match"][tvalues[i][0]]=tvalues[i][1];
       terms.push(terms_temp);
        
        }
        else {
        //if not boolean, exact match terms query
        var terms_temp={"terms":{}};
        terms_temp["terms"][tvalues[i][0].concat('.keyword')]=[tvalues[i][1]];
       terms.push(terms_temp);
      }
      }
   }
  
  
  //filters ranges queries
  for (var i = 0; i < fvalues.length; i++) {
      if (fvalues[i][0].length>0 & JSON.stringify(fvalues[i][1]).length>0 & JSON.stringify(fvalues[i][2]).length>0){
        
        //if not boolean, exact match terms query
        var range_temp={"range":{}};
        range_temp["range"][fvalues[i][0]]={};
        range_temp["range"][fvalues[i][0]][fvalues[i][1]]=fvalues[i][2];
        
       filters.push(range_temp);
      }
      }  
  
  
    if (terms.length>0 | filters.length>0){
    
      query["query"]={"bool":{}};
      
      if (terms.length>0){
        
        query["query"]["bool"]["must"]=
          
          terms;
      }
      
      if (filters.length>0){
        query["query"]["bool"]["filter"]=filters;
      
      }
    
  }
  

  url=url.concat('/',index,'/_search?pretty=true&size=',ndocs);
  
  query=JSON.stringify(query)
  
  Logger.log(query);

  
var response = UrlFetchApp.fetch(url, {
            "method": "get",
            'contentType': 'application/json',
            "headers": {
                "Authorization": "Basic " + Utilities.base64Encode(basic_auth)
                },
  "payload": query
  
            });

  
  
  
  // Make request to API and get response before this point.
  var json = response.getContentText();
   
  var sheet = SpreadsheetApp.getActiveSheet();
  var data = JSON.parse(json);
  
  var results = data['hits']['hits'];
  var output = []
  
  var headers = Object.keys(results[0]._source)
  headers.push('_id')
  
  output.push(headers)
  
  results.forEach(function(elem,i){
  
    var vals = Object.values(elem._source)
    vals.push(elem._id)
    output.push(vals);
  
  
  });
  
  var nrows = output.length;
  var ncols = headers.length;
  
    //range and last row with data
  var actual_data_range = sheet.getDataRange();
var last_row = actual_data_range.getLastRow();
var last_column = actual_data_range.getLastColumn();

var first_data_row=18 //start reading data from here
var first_data_column=1
  
  sheet.getRange(first_data_row,first_data_column,last_row,last_column).clearContent();
  // paste in the values
  sheet.getRange(first_data_row,first_data_column,nrows,ncols).setValues(output);
  
   // adds an index number to the array
 // output.forEach(function(elem,i) {
 //   elem.unshift(i + 1);
 // });
  
 
  //sheet.getRange(1,1).setValue();
  
  // Return only the data we're interested in. 
  //return data['hits']['hits'][0]['_source'];
}