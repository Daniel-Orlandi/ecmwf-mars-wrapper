from os import stat_result
from ecmwfapi import ECMWFDataServer
import multiprocessing
import pandas
import json

from pandas.core.indexes.datetimes import date_range


class ECMWF_retriever:
    def __init__(self, date_begin:str = None, date_end:str = None, out_dir:str = None) -> None:
        self.date_begin = date_begin
        self.date_end = date_end            
        self.date_list = None
        self.out_dir = None    

    def generate_dates(self, week_filter:list = None, freq:str = 'D'):
        dates = pandas.date_range(start = self.date_begin,
                                  end = self.date_end,
                                  freq = freq )

        if (week_filter is not None):
            week_day_map = {
                            'sunday':6,
                            'monday':0,
                            'tuesday':1,
                            'wednesday':2,
                            'thursday':3,
                            'friday':4,
                            'saturday':5,
                            }

            filtered_dict = {}
            for weekday in week_filter:
                filtered_dict[weekday] = dates[dates.dayofweek==week_day_map[weekday]]

            keys_list = list(filtered_dict.keys())

            temp_list = []
            for week_day in keys_list:
                data = filtered_dict[week_day].to_list()
                temp_list = temp_list+data        
               
        date_list = [str(date.strftime('%Y-%m-%d')) for date in temp_list]
        date_list.sort()       
        return date_list
    
    @staticmethod
    def set_request_list(config_dict:dict):
        req_list = []
        for request_name, data_request in config_dict.items():
            req_list.append(data_request)

        return req_list   

    @staticmethod
    def load_config_file(file_path: str) -> dict:
        try:
            with open(file_path, 'r') as j:
                data_dict = json.load(j)       
            return data_dict

        except FileNotFoundError as file_not_found_error:
            print(f'Error: {file_not_found_error}')
        
        except Exception as general_error:
            print(f'Error: {general_error}')

    
    def retrieve(self, request_config:dict) -> None:
        """
        An ECMWF reforecast, perturbed forecast, pressure level, request.
        Change the keywords below to adapt it to your needs. (eg to add or remove some steps or parameters etc)
        """
        try:
            if (isinstance(self.date_begin, str)):
                if(isinstance(self.date_end,str) is not True):
                    raise ValueError ("If date_begin is provided, you must provide date_end!")

                if (isinstance(self.out_dir,str)):
                    target = self.out_dir + f"{request_config['origin']}_{self.date_begin}-{self.date_end}_{request_config['levtype']}.grb"
                else:
                    target = f"{request_config['origin']}_{self.date_begin}-{self.date_end}_{request_config['levtype']}.grb" 

                

            else:
                if (isinstance(self.out_dir,str)):
                    target = self.out_dir + f"{request_config['origin']}_-_{request_config['levtype']}.grb"

                else:
                    target = f"{request_config['origin']}_-_{request_config['levtype']}.grb" 

            server = ECMWFDataServer()                
            request_config['target'] = request_config['target'] +'_'+target 
            server.retrieve(request_config)
        
        except Exception as general_error:
            print(f'Error: {general_error}')

    def sequential_retrieve(self, config_dict: dict):
        req_list = self.set_request_list(config_dict)

        for req in req_list:
            self.retrieve(req)
        print('Done!')
        
    def parallel_retrieve(self, config_dict: dict, num_cores = multiprocessing.cpu_count()/4):
        req_list = self.set_request_list(config_dict)

        with multiprocessing.Pool(processes=num_cores) as pool:            
                pool.map(self.retrieve,req_list)        
        print('Done!')





 
    