from functools import partial
import cdsapi
import multiprocessing

from src.tools.data_tools import dataTools

class cdsRetriever:

    @staticmethod
    def retrieve(request_config:dict, date_begin:str =None ,date_end:str = None, out_dir:str = None) -> None:
        
        try:
            print(date_begin, date_end, out_dir)
            if (isinstance(date_begin, str)):
                if(isinstance(date_end,str) is not True):
                    raise ValueError ("If date_begin is provided, you must provide date_end!")

                if (isinstance(out_dir,str)):
                    target = out_dir + f"{request_config['request']['variable']}_{date_begin}-{date_end}.grb"

                else:
                    target = f"{request_config['request']['variable']}_{date_begin}-{date_end}.grb" 
            
            else:
                if (isinstance(out_dir,str)):
                    target = out_dir + f"{request_config['request']['variable']}.grb"

                else:
                    target = f"{request_config['request']['variable']}.grb"  

            request_config['target'] = target
            server = cdsapi.Client()
            server.retrieve(request_config['request'],request_config['request'], request_config['target'])
        
        except Exception as general_error:
            print(f'Error: {general_error}')

    def sequential_retrieve(self, config_dict: dict,**kwargs):
        req_list = dataTools().set_request_list(config_dict)

        for req in req_list:
            self.retrieve(req, **kwargs)

        print('Done!')
        
    def parallel_retrieve(self, config_dict: dict, num_cores = int(multiprocessing.cpu_count()/4),**kwargs):
        req_list = dataTools().set_request_list(config_dict)

        with multiprocessing.Pool(processes=num_cores) as pool:            
                pool.map(partial(self.retrieve, **kwargs), req_list)   

        print('Done!')





 
    
    