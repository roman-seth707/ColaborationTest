from Raptor.Leggero.Leggero_Model_Reflection import LeggeroApplicationDB
from Raptor.commons.config_reader import LgConfig
from Raptor.dbserver.Leggero_Model import *
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk,parallel_bulk
from memory_profiler import profile
import os
import json
from datetime import datetime

class DataTableIterator:

    """ 
    This is a class for retrieve huge dataset and indexing data to elastic serach
      
    Attributes: 
        batchSize (int): batch size 
        file_name(string): log file name
        folder_name(string): folder containing log file
    """

    batchSize = 0
    doc_success, doc_fail = 0, 0
    error = None
    response = {}
    # response = {'doc_success':0,'doc_fail':0,'total_execution_time':0}
    file_name = "/log1.txt"
    folder_name = "/log10"
    def __init__(self,index_name, document_type):
        """ 
        The constructor is used to initialize database connection.    
        """
        self.db_obj = LeggeroApplicationDB('LeggeroDB')
        self.BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + self.folder_name
        self.file_obj = open(self.BASE_DIR + self.file_name, 'a+')  # append mode
        #Elastic configuration
        self.index_name = index_name
        self.document_type = document_type
        
   
    @profile
    def get_data_iterative(self,query,batch_size = 1000):
        """
        This is the main function whihc fetch data in batch and call varirous related function inorder to insert data in elastic serach
        """
        proxy = None
        start_time = datetime.now() # Only for execution analysis
        self.batchSize = batch_size
        # lgc = LgConfig()
        _ladb = LeggeroApplicationDB('LeggeroDB')
        con = _ladb.get_dbengine()
        # proxy = con.execution_options(stream_results=True).execute('select * from leggero.sales_test1;')
        proxy = con.execution_options(stream_results=True).execute(query)
        batch_count = 0
        print("Batch size:",self.batchSize)
        dataList = []
        try:
            while True:
                dataList = []
                batch = proxy.fetchmany(self.batchSize)  # 100,000 rows at a time
                if not batch:
                    break
                batch_count +=1
                print("Batch_count:",batch_count)
                for row in batch:
                    # obj = self.format_sales_data(row)
                    obj = self.map_object(row)
                    dataList.append(self.elastic_format(obj, row.id))
                # self.parallel_to_elasic(data = dataList) # Save to Elastic
                self.streaming_to_elasic(data = dataList) # Save to Elastic
                
        
        except Exception as e:
            print("==============Exception Occure=============")
            print("I/O error({0})".format(e.message))
            self.error = str(e)
            self.create_log(exception_error = self.error)
            
        finally:
            print("===========Finally block==========")
            proxy.close()
            self.file_obj.close()
            self.response = self.set_response(start_time = start_time)
            return self.response
            

    def format_sales_data(self,row):
        """
        This function takes slqAlchemy object and set object attribute into dictionari

        Parameters:
        dictionaryData(sqlAlchemy object) : row data

        Returns: 
            dict: return dictionary.
        """
        return {
             'area_type':row.area_type,
             'availability':row.availability,
             'location':row.location,
             'size':row.size,
             'society':row.society,
             'total_sqft':row.total_sqft,
             'bath':row.bath,
             'balcony':row.balcony,
            'price':row.price,
            'id':row.id
            }
    
    def map_object(self, row, isObject = False):
        """
        This function is used to set table columns and its associated values

        Parameters:
        row((object) : SqlAlchemy row object
        isObject(boolean) : weather it is an Object of class or cursor

        Return:
        dictionary

        """
        maped_data = {}
        if isObject:
            for column in row.__table__.columns:
                maped_data[column.name] = (getattr(row, column.name))    #str(getattr(row, column.name))
                
        else:
            dict1 = dict(row)
            for col in dict1:
                maped_data[col] = dict1[col]
        
        
        return maped_data

    def elastic_format(self,obj,id):
        """
        This function takes object and Uique id and set format as per elasticsearch

        Parameters:
        dictionaryData(sqlAlchemy object) : dictionary data
        id(int) :  row id

        Returns: 
            dict: return dictionary.
        """
        return {"_index":self.index_name, "_type" : self.document_type,"_id": id,"_source":obj}

        """
        This function is used to index data into elasticsearch using python bulk helper

        Parameters:
        dictionaryData((List of dict) : list of document/records

        """
        elasticObj = Elasticsearch()
        print("In elastic")
        print("Document Length:",len(dataList))
        

        for success ,action in parallel_bulk(client=elasticObj, actions=dataList,thread_count=3,chunk_size=500):
            if success:
                self.doc_success += 1
            else:
                self.doc_fail +=1
                self.create_log(data = action)

    
    def parallel_to_elasic(self,data,thread = 3,chunk = 500):
        """
        The parallel_bulk() api is a wrapper around the bulk() api to provide threading. 
        parallel_bulk() returns a generator which must be consumed to produce results.

        Parameters:
        dictionaryData((List of dict) : list of document/records
        thread(int) : Number of Thread
        chunk(int) : Chunk_size is used to get data from iterator
        """
        elasticObj = Elasticsearch()
        print("Parallel Elastic")
        print("Document Length:",len(data))
        for success ,action in parallel_bulk(client=elasticObj, actions=data,thread_count=thread,chunk_size=chunk):
            if success:
                self.doc_success += 1
            else:
                self.doc_fail +=1
                self.create_log(data = action)


    def streaming_to_elasic(self,data,max_retries = 3):
        """
        This fStreaming bulk consumes actions from the iterable passed in and yields results per action.

        Parameters:
        dictionaryData((List of dict) : list of document/records
        max_retries(int) : retry to insert data which is failed during first attmpt
        """
        elasticObj = Elasticsearch()
        print("Streaming elastic")
        print("Document Length:",len(data))
        c = 0
        progress_print=1000000000
        sleep_time = 2
        for ok, action in streaming_bulk(client=elasticObj, index=self.index_name,
                                         actions=data,
                                         max_retries=max_retries):
            c += 1
            if ok:
                self.doc_success += 1
            else:
                self.doc_fail +=1
                self.create_log(data = action)
                
            if c % progress_print == 0:
                time.sleep(sleep_time)
                print 'done', c
            


    def create_log(self,data = None,exception_error = None):
        """
        This function is used to create log for those ducument which are not inserted in elastic serach due to some reasons
        Parameters:
        data((dict) : unindexed document
        """
        if not os.path.exists(self.BASE_DIR):
            os.makedirs(self.BASE_DIR )
        self.file_obj.writelines("\n") 
        self.file_obj.writelines("="*30 + str(datetime.today()) +"="*30 + "\n" ) 
        if data:
            self.file_obj.writelines(json.dumps(data)) 
        if exception_error:
            self.file_obj.writelines(exception_error) 
        
   
        
    @profile
    def iterate_data(self, table_class,limit = 500):
        """
        This function is used to retrieve and insert data into elasticsearch

        Parameters:
        table_class((Class) : SqlAlchemy Table class
        limit(int) : number of records need to be fetched from database

        Return:
        dictionary
        """
        start_time = datetime.now() # Only for execution analysis
        sess = self.db_obj.get_session()
        query = sess.query(table_class)  #S alesTest1
        page = 0
        flag = True
        try:
            while flag:
                dataList = []
                flag = False
                print("Page:",page)
                records =  query.offset(page*limit).limit(limit) #self.fetch_data(query,page=page, page_size=limit)
                for row in records:
                    flag = True
                    cleaned_data = self.map_object(row,isObject = True)
                    # cleaned_data = self.format_sales_data(row)
                    dataList.append(self.elastic_format(cleaned_data, row.id))

                # self.parallel_to_elasic(data = dataList) # Save data to elastic
                self.streaming_to_elasic(data = dataList) # Save to Elastic
                page +=1
               
        except Exception as e:
            print("==============Exception Occure=============")
            print("I/O error({0})".format(e))
            self.error = str(e)
            self.create_log(exception_error = self.error)
        finally:
            print("===========Finally block==========")
            self.file_obj.close()  
            self.response = self.set_response(start_time = start_time)
            return self.response

    def execution_time(self, star_time):
        """
        This function is used to calculate time consumed by called function

        Parameters:
        star_time((dattime) : Starting time of a function

        Return:
        int
        """
        end_exe_time = datetime.now()
        total_taken_time = end_exe_time - star_time
        print("===========Time Analysis=================")
        # print("Program Execution Time(miliseconds):",total_taken_time.total_seconds() * 1000) #milliseconds
        return total_taken_time.seconds
    
    def set_response(self, start_time):
        return {
           'doc_success':self.doc_success,
           'doc_fail':self.doc_fail,
           'total_execution_time':self.execution_time(start_time),
           'error': self.error
           }

if __name__ == "__main__":
    obj = DataTableIterator(index_name = 'org2',document_type = 'organization')
    # query = 'select * from leggero.test3;'
    # resp  = obj.get_data_iterative(query = query, batch_size = 200)
    #python -m memory_profiler example.py
    resp  = obj.iterate_data(table_class = ORG_Test5, limit = 1000)
    print(resp)
   
  


"""
index_name = 'sales1'       # index = titanic,sales1,sales2,org2, reatl1,reatl2
document_type = 'sale'    # titanic(passenger),sales1(sale),org2(organization), retail1(retail_stock)

"""

