import multiprocessing
from functools import partial
from ecmwfapi import ECMWFDataServer

from src.tools.data_tools import dataTools

class ecmwfRetriever:    

    @staticmethod
    def retrieve(request_config:dict, date_begin:str =None ,date_end:str = None, date_list:list = None, out_dir:str = None) -> None:
        """
        An ECMWF reforecast, perturbed forecast, pressure level, request.
        Change the keywords below to adapt it to your needs. (eg to add or remove some steps or parameters etc)
        """
        try:
            if (isinstance(date_begin, str)):
                if(isinstance(date_end,str) is not True):
                    raise ValueError ("If date_begin is provided, you must provide date_end!")

                if (isinstance(out_dir,str)):
                    target = out_dir + f"{request_config['origin']}_{date_begin}-{date_end}_{request_config['levtype']}.grb"
                else:
                    target = f"{request_config['origin']}_{date_begin}-{date_end}_{request_config['levtype']}.grb" 
            
            else:
                if (isinstance(out_dir,str)):
                    target = out_dir + f"{request_config['origin']}_-_{request_config['levtype']}.grb"

                else:
                    target = f"{request_config['origin']}_-_{request_config['levtype']}.grb"

            server = ECMWFDataServer()
            
            if (isinstance(date_list, list)):
               request_config['date'] = date_list

            request_config['target'] = request_config['target'] +'_'+target 
            server.retrieve(request_config)
        
        except Exception as general_error:
            print(f'Error: {general_error}')


    def sequential_retrieve(self, config_dict: dict,**kwargs):
        req_list = dataTools().set_request_list(config_dict)

        for req in req_list:
            self.retrieve(req, **kwargs)
        print('Done!')
        

    def parallel_retrieve(self, config_dict: dict, num_cores = multiprocessing.cpu_count()/4,**kwargs):
        req_list = dataTools().set_request_list(config_dict)

        with multiprocessing.Pool(processes=num_cores) as pool:            
                pool.map(partial(self.retrieve, **kwargs),req_list)        
        print('Done!')





 
    
    