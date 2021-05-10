# ecmwf-mars-wrapper
Simple python wrapper to simplify  data retrieval from ECMW mars api.

   
Python CLI tool to download data from ECMWF's mars web api. 
To use the program you should privide a path to a json file with one or more 
data request in valid MARS format in it i.e: 

[JSON file structure]

 "request_config_0":{
    "class": "s2",
    "dataset": "s2s",
    "date": "2021-03-01/2021-03-04/2021-03-08/2021-03-11/2021-03-15/2021-03-18/2021-03-22/2021-03-25/2021-03-29",
    "expver": "prod",
    "levtype": "sfc",
    "model": "glob",
    "origin": "ecmf",
    "param": "228228",
    "step": "",
    "stream": "enfo",
    "time": "00:00:00",
    "type": "cf",
    "area":"5/-90/-60/-30",
    "target": "output"
},

"request_config_1":{
    "class": "s2",
    "dataset": "s2s",
    "date": "2021-03-01/2021-03-04/2021-03-08/2021-03-11/2021-03-15/2021-03-18/2021-03-22/2021-03-25/2021-03-29",
    "expver": "prod",
    "levtype": "sfc",
    "model": "glob",
    "origin": "ecmf",
    "param": "228228",
    "step": "",
    "stream": "enfo",
    "time": "00:00:00",
    "type": "cf",
    "area":"5/-90/-60/-30",
    "target":"output" 
}

more info on how to get a valid ECMWF data request on: https://www.ecmwf.int/en/forecasts/access-forecasts/ecmwf-web-api
Usage example:
downloader.py <path-to-request-file>

   
