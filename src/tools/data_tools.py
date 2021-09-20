import pandas
import json


class dataTools:

  @staticmethod
  def generate_dates(date_begin:str, date_end:str, week_filter:list = None, freq:str = 'D')->list:
        try:
          dates = pandas.date_range(start = date_begin,
                                    end = date_end,
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

        except Exception as general_error:
          raise general_error      
        
        else:
           return date_list


  @staticmethod
  def set_request_list(config_dict:dict):
    try:
      req_list = []
      for request_name, data_request in config_dict.items():
          req_list.append(data_request)

    except Exception as general_error:
      raise general_error
    
    else:
      return req_list  


  @staticmethod
  def load_config_file(file_path: str) -> dict:
      try:
          with open(file_path, 'r') as j:
                      data_dict = json.load(j)    
      
      except FileNotFoundError as file_not_found_error:
          print(f'Error: {file_not_found_error}')
      
      except Exception as general_error:
          print(f'Error: {general_error}')
      
      else:
            return data_dict
