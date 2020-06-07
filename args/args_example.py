from modules.Elastic_Module import *



blacklist1=[]
blacklist2=[]


ES_HOST = ""
basic_auth={'user':"", 'psswd':""}
es=elastic_connection(ES_HOST,basic_auth=basic_auth)
print('testing elastic connection')
es.ping()


args={
    "data_dict_title":"Example Data Dictionary", 
      "rootdir":"_static/",
      "source_path":"source/index.rst",
      "sections_path":"source/_sections/{}.rst",
      "dbpaths":"<../_sections/{}>",
      "INDEXpaths":"<../_sections/{}>",
      "tables_rst_path":"../_static/tables/{}.html",
      "actual_tables_path":"source/_static/tables/{}.html",
      "elastic_connection":es,
      "links":[],
      "description":"",
      
      "index_template":"templates/index.rst",
                    "section_template":"templates/section.rst",
                    "sections_template":"templates/sections.rst",
      "sep":"---",
      
      "DBases":{ 1: {
          
          "DBname":"Elasticsearch",
          "DBconnection": es,
          "blacklist":blacklist1,
          "DBdescription":"First DB Example",
          "DBlinks": []
          
          
      },  2:{
                
          "DBname":"Elasticsearch 2",
          "DBconnection": es,
          "blacklist":blacklist2,
          "DBdescription":"Second DB Example",
          "DBlinks":[]
                   
         }
          
      }
     }