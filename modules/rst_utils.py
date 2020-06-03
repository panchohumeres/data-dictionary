import jinja2
from modules.unwind import *
import pandas as pd
from modules.Elastic_Module import *

class rst:
    
    def __init__(self,kwargs):
        self.__dict__.update(**kwargs)
        
        if not self.has_attribute('index_template'):
            self.index_template='templates/index.rst'

        if not self.has_attribute('section_template'):
            self.section_template='templates/section.rst'
            
        if not self.has_attribute('section_template'):
            self.section_template='templates/sections.rst'
            
        if not self.has_attribute('data_dict_index'):
            self.data_dict_index=[
    ('data_dict','data'),
        ]
            
        if not self.has_attribute('elastic_args'):
            self.elastic_args={'n':1500,'id_field':'id'}
            
        if not self.has_attribute('sep'):
            self.sep='---'
            
        self.paths_pattern()
        self.unwind_docs(sep=self.sep)
        self.create_tables()
        self.render_patterns()
        self.save_rendered_templates()
        self.save_to_elastic(**self.elastic_args)
   
    def paths_pattern(self):
        self.create_index_paths()
        self.create_sections_refs()
        self.create_table_paths()
        
    def render_patterns(self):
        self.load_templates()
        self.render_index()
        self.render_sections()
        
    def create_index_paths(self):
        DBases=self.DBases
        for DB in DBases.keys():
            DBases[DB]['path']=self.sections_path.replace('{}',DBases[DB]['DBname']).replace(' ','_')
            DBases[DB]['rst_path']=self.dbpaths.replace('{}',DBases[DB]['DBname']).replace(' ','_')
            DBases[DB]['section_entry']=str(DB)+'. '+DBases[DB]['DBname']+' '+DBases[DB]['rst_path']
        self.DBases=DBases 
               
    def create_sections_refs(self):
        DBases=self.DBases
        for DB in DBases.keys():
            self.DBases[DB]['indexes']={}
            es=self.DBases[DB]['DBconnection']
            #get list of existing indexes or collections in Elasticsearch or MongoDB
            blacklist=self.DBases[DB]['blacklist']
            indexes=[i for i in es.list_indices() if i not in blacklist]
            idxs={}
            for INDEX in indexes:
                self.DBases[DB]['indexes'][INDEX]={}           

        
    def create_table_paths(self):
        DBases=self.DBases
        for DB in DBases.keys():
            DBname=self.DBases[DB]['DBname']
            indexes=self.DBases[DB]['indexes'].keys()
            idxs=list(range(1,len(indexes)+1))
            for INDEX,idx in zip(indexes,idxs):
                self.DBases[DB]['indexes'][INDEX]['rst_table_path']=self.tables_rst_path.replace('{}',str(DBname)+'-'+str(INDEX)).replace(' ','_')
                self.DBases[DB]['indexes'][INDEX]['actual_table_path']=self.actual_tables_path.replace('{}',str(DBname)+'-'+str(INDEX)).replace(' ','_')
                self.DBases[DB]['indexes'][INDEX]['section_path']=self.sections_path.replace('{}',str(DBname)+'-'+str(INDEX)).replace(' ','_')
                self.DBases[DB]['indexes'][INDEX]['rst_section_path']=self.INDEXpaths.replace('{}',str(DBname)+'-'+str(INDEX)).replace(' ','_')
                rst_path=self.DBases[DB]['indexes'][INDEX]['rst_section_path']
                self.DBases[DB]['indexes'][INDEX]['section_entry']=str(idx)+'. '+INDEX+' '+rst_path
                
   
    def load_templates(self):
        self.index_template=self.open_file(self.index_template)
        self.section_template=self.open_file(self.section_template)
        self.sections_template=self.open_file(self.sections_template)
        
        
    def render_index(self):
        idx=[]
        keys=list(self.DBases.keys())
        keys.sort()
        databases=[]
        for DB in keys:
            databases.append(self.DBases[DB]['section_entry'])
        self.rendered_index=self.index_template.render(title=self.data_dict_title,databases=databases)
        
        
    def render_sections(self):
        DBases=self.DBases
        for DB in DBases.keys():
            DBname=self.DBases[DB]['DBname']
            DBdescription=self.DBases[DB]['DBdescription']
            
            indexes=[]
            for INDEX in self.DBases[DB]['indexes'].keys():
                entry=DBases[DB]['indexes'][INDEX]['section_entry']
                indexes.append(entry)
                path=DBases[DB]['indexes'][INDEX]['rst_table_path']
                self.DBases[DB]['indexes'][INDEX]['rendered']=self.section_template.render(INDEX=INDEX,DBname=DBname,path=path)
                
                    
            self.DBases[DB]['rendered']=self.sections_template.render(indexes=indexes,DBname=DBname,DBdescription=DBdescription)
            
            
    def save_rendered_templates(self):
        DBases=self.DBases
        path=self.source_path
        content=self.rendered_index
        self.save_file(path,content)
        for DB in DBases.keys():
            content=self.DBases[DB]['rendered']
            path=self.DBases[DB]['path']
            self.save_file(path,content)
            for INDEX in self.DBases[DB]['indexes'].keys():
                path=self.DBases[DB]['indexes'][INDEX]['section_path']
                content=self.DBases[DB]['indexes'][INDEX]['rendered']
                self.save_file(path,content)

            
            
    def unwind_docs(self,sep='---'):
        DBases=self.DBases
        dfs=[]
        for DB in DBases.keys():
            es=self.DBases[DB]['DBconnection']
            for INDEX in self.DBases[DB]['indexes'].keys():
                docs=es.test_index(INDEX,n=1,as_df=False)
                unwinded=unwind(docs=[docs['hits']['hits'][0]['_source']],sep=sep)
                df=unwinded.docs
                df['index-collection']=INDEX
                df['DB']=self.DBases[DB]['DBname']
                df.index=df['DB']+self.sep+df['index-collection']+self.sep+df['key']
                df=df.fillna('')
                dfs.append(df)
                
        self.master_df=pd.concat(dfs)
        

    def open_file(self,rst_path):
        
        with open(rst_path, 'r') as f:
            f= f.read()
            f=jinja2.Template(f)
            
        return f
    
    def save_file(self,rst_path,content):
        file = open(rst_path, 'w')
        file.write(content)
        file.close()
    
    def create_tables(self):
        DBases=self.DBases
        df=self.master_df
        for DB in DBases.keys():
            DBname=self.DBases[DB]['DBname']
            #for every database (hierchy level 1)
            #slice the df for that DB in particular
            slc=df[df['DB']==DBname]
            for idx in slc['index-collection'].unique(): #hierarchy level 2
                #for every index or collection that particular database
                #within the particular DB slice, sub-slice for that particular index o or collection
                index=slc[slc['index-collection']==idx]
                keys=[k for k in index.columns if 'key(' in k] #Number of nested keys that the df has
                path=self.DBases[DB]['indexes'][idx]['actual_table_path']
                print('saving table: '+path)
                pd.DataFrame(index.set_index(keys))[['key','value','type','description']].to_html(path)
                
    def sections_paths(self):
        DBases=self.DBases
        for DB in DBases.keys():
            DBname=self.DBases[DB]['DBname']
            DBdescription=self.DBases[DB]['DBdescription']
            
            databases=[]
            databases.append(self.DBases[DB]['section_entry'])
            self.rendered_sections=self.sections_template.render(title=self.data_dict_title,databases=databases)

            DBases[DB]['path']=self.sections_path.replace('{}',DBases[DB]['DBname']).replace(' ','_')
            DBases[DB]['section_entry']=str(DB)+'. '+DBases[DB]['DBname']+' '+DBases[DB]['path']            

    def has_attribute(self,attribute):
        return (hasattr(self, attribute))
    
    
    def save_to_elastic(self,id_field='',n=''):
        data=self.master_df
        data[id_field]=data.index
        es=self.elastic_connection
        es.add_docs(data)
        print('N°Documents before indexing :'+str(data.shape[0]))
        es.elastic_prepare_pattern()
        indexes=self.data_dict_index
        #generate bulk data
        print('generating bulk data for indexing data dict into Elasticsearch')      
        i=indexes[0]
        es.bulk_data(INDEX_NAME=i[0],_type=i[1],n=n,id_field=id_field)
        print('clearing index')
        es.clear_index(i[0])
        print('executing bulk index')
        es.bulk_index()
        print('safety check: counting indexed documents')
        es.count_documents(i[0])