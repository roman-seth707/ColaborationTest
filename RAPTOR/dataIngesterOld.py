from Raptor.Leggero.Leggero_Model_Reflection import LeggeroApplicationDB
from Raptor.commons.config_reader import LgConfig
from Raptor.dbserver.Leggero_Model import *
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from memory_profiler import profile
import os
import json

class DataTableIterator:

    """ 
    This is a class for retrieve huge dataset and indexing data to elastic serach
      
    Attributes: 
        batchSize (int): batch size 
        index_name (string): index name in elastic search
        index_type (int): document type in index
    """

    batchSize = 50
    index_name = 'sales1'       # index = titanic,sales1,sales2
    index_type = 'sale'    # titanic(passenger),sales1(sale)
    file_name = "/log1.txt"
    folder_name = "/log10"
    def __init__(self):
        """ 
        The constructor is used to initialize database connection.    
        """
        self.db_obj = LeggeroApplicationDB('LeggeroDB')
        self.BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + self.folder_name
        self.file_obj = open(self.BASE_DIR + self.file_name, 'a')  # append mode
    
    def get_data_iterativeOld(self, limit, offset):
        sess = self.db_obj.get_session()
        cc = sess.query(Test3).partitions()
        i = 0 # count Row
        dataList = []
        # for row in sess.query(Test3).yield_per(self.batchSize).enable_eagerloads(False):
        #     print(row.name)
        #     i +=1
        #     obj = {'name':row.name,'sex':row.sex,'age':row.age,'fare':row.fare,'cabin':row.cabin,'embarked':row.embarked}
        #     dataList.append(obj)
        #     print("i:",i)
    @profile
    def get_data_iterative(self):
        """
        This is main function whihc fetch data in batch and call varirous related function
        """
        lgc = LgConfig()
        _ladb = LeggeroApplicationDB('LeggeroDB')
        con = _ladb.get_dbengine()
        proxy = con.execution_options(stream_results=True).execute('select * from leggero.sales_test1 limit 100;')
        batch_count = 0
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
                    obj = self.format_sales_data(row)
                    dataList.append(self.elastic_format(obj, row.id))
                self.save_to_elasic(dataList) # Save to Elastic
        except Exception as e:
            print("==============Exception Occure=============")
            print("I/O error({0})".format(e))
            
        finally:
            print("===========Finally block==========")
            proxy.close()
            self.file_obj.close()  

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
            'price':row.price
            }

    def elastic_format(self,obj,id):
        """
        This function takes object and Uique id and set format as per elasticsearch

        Parameters:
        dictionaryData(sqlAlchemy object) : dictionary data
        id(int) :  row id

        Returns: 
            dict: return dictionary.
        """
        return {"_index":self.index_name, "_type" : self.index_type,"_id": id,"_source":obj}

    # def get_data(self):
    #     lgc = LgConfig()
    #     _ladb = LeggeroApplicationDB('LeggeroDB')
    #     con = _ladb.get_dbengine()
    #     # q = select('select * from leggero.test3;')
    #     proxy = con.execution_options(stream_results=True).execute('select * from leggero.test3;')
    #     proxy = con.execution_options(stream_results=True).execute('select * from leggero.test3;')
    #     batch_count = 0
    #     dataList = []
    #     while True:
    #         dataList = []
    #         batch = proxy.fetchmany(self.batchSize)  # 100,000 rows at a time
    #         if not batch:
    #             break
    #         batch_count +=1
    #         print("Batch_count:",batch_count)
    #         for row in batch:
    #             obj = {'name':row.name,'sex':row.sex,'age':row.age,'fare':row.fare,'cabin':row.cabin,'embarked':row.embarked}

    #             dataList.append(self.elastic_format(obj, row.id))
    #         self.save_to_elasic(dataList) # Save to Elastic
           
    #     proxy.close()
    

    def save_to_elasic(self,dataList):
        """
        This function is used to index data into elasticsearch using python bulk helper

        Parameters:
        dictionaryData((List of dict) : list of document/records

        """
        elasticObj = Elasticsearch()
        print("In elastic")
        print("List Length:",len(dataList))
        errorObj = {'name':'khan','bana':10101}
        dataList.append(errorObj)
        c, success = 0, 0
        fail = 0
        progress_print=1000000000
        sleep_time = 2
        for ok, action in streaming_bulk(client=elasticObj, index=self.index_name,
                                         actions=dataList,
                                         max_retries=3):
            c += 1
            print(c)
            if ok:
                success += 1
                print 'success', success
                self.create_log(action)
            # if c % progress_print == 0:
            #     time.sleep(sleep_time)
            #     print 'done', c
            #     print 'success', success
            else:
                fail += 1
                print 'fail', fail

    def create_log(self,data):
        """
        This function is used to create log for those ducument which are not inserted in elastic serach
        Parameters:
        dictionaryData((dict) : unindexed document
        """
        if not os.path.exists(self.BASE_DIR):
            os.makedirs(self.BASE_DIR )
        self.file_obj.writelines("\n") 
        self.file_obj.writelines(json.dumps(data)) 
    
        

if __name__ == "__main__":
    # lgc = LgConfig()
    # _ladb = LeggeroApplicationDB('LeggeroDB')
    # con = _ladb.get_dbengine()
    # rec =  con.execute('select * from leggero.test3;')
    # x = rec.fetchmany(50)
    # flag  = True
    # while flag:
    #     result = rec.partitions(size =50)
    #     if (result == []):
    #         flag = False
    #     else:
    #         print(result)
  
    # rec.close()
    # for r in rec:
    #     print r
    obj = DataTableIterator()
    obj.get_data_iterative()
    # obj.create_log()



