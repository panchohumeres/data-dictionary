function put_elastic_data(){
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
  
  //"type" of documents
  var type_cell='B8';
  var idx_type=SpreadsheetApp.getActiveSheet().getRange(type_cell).getValue();
  
  //chunks size for bulk index
  var ndocs_cell='B8';
  var ndocs=SpreadsheetApp.getActiveSheet().getRange(ndocs_cell).getValue();
  
  
  //basic auth params
  var basic_auth=user.concat(':',psswd);
  
//queries params
var q_range_1='E4';
var q_range_2='F6';

  var q_range= q_range_1.concat(':',q_range_2)
  
var ss = SpreadsheetApp.getActiveSpreadsheet();
var sheet = ss.getActiveSheet();

      //range and last row with data
  var actual_data_range = sheet.getDataRange();
var last_row = actual_data_range.getLastRow();
var last_column = actual_data_range.getLastColumn();

var first_data_row=18 //start reading data from here
var first_data_column=1
  
  var range=sheet.getRange(first_data_row,first_data_column,last_row,last_column);
  var values = range.getValues();
  
  var headers=values[0] //cabeceras
  const _id_idx = headers.findIndex(id => id === "_id");
  
  var values=values.slice(1); //datos

  function _range(start, stop, step) {
    var a = [start], b = start;
    while (b < stop) {
        a.push(b += step || 1);
    }
    return a;
  }
  
  var idxs=_range(0,headers.length-1,1)
  
  var records=[];
  
  for (var i = 0; i < values.length; i++) {
    
   var _id=values[i][_id_idx];
   records.push({"index": {"_index": index, "_type": idx_type, "_id": _id}});
    
   var record=values[i] 
    var r={};
   headers.forEach((header, idx) => {
                  
                   if (header!='_id' & header!=''){             
  r[header]=record[idx];                         
   }
  });
    
    records.push(r);
 
}
  
url=url.concat('/',index,'/_bulk');

for (var i = 0; i < records.length; i++) {
  
  records[i]=JSON.stringify(records[i])
  }

 records=records.join("\n");
  records=records.concat("\n");


 
var response = UrlFetchApp.fetch(url, {
           "method": "post",
            'contentType': 'application/json',
          "headers": {
               "Authorization": "Basic " + Utilities.base64Encode(basic_auth)
               },
  "payload": records
  
           });

Logger.log(records);
   // adds an index number to the array
 // output.forEach(function(elem,i) {
 //   elem.unshift(i + 1);
 // });
  
 
  //sheet.getRange(1,1).setValue();
  
  // Return only the data we're interested in. 
  //return data['hits']['hits'][0]['_source'];
}