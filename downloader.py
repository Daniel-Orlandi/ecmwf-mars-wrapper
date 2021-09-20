import click
from src.model_data_retriever.ecmwf_request import ecmwfRetriever
from src.model_data_retriever.cds_request import cdsRetriever
from src.tools.data_tools import dataTools
@click.command()

@click.argument(
    'request-file',
    type=click.Path(exists=True))

@click.option(
    '--dates',
    '-d',
    nargs=2,
    type=str,
    help= "date_begin and date_end of request. If not provided application will use date field from request file."
)

@click.option(
    '--week_filter',
    '-wf',
    multiple=True,
    type=str,
    help= "If"
)

@click.option(
    '-parallel',
    '-p',
    is_flag=True,    
    help= "If passed, parallel processing is applied."
)

@click.option(
    '--num-cores',
    '-nc',
    show_default=True,
    type=int,
    help= "Number of cpu cores to be used in application"
)

@click.option(
    '--mode',    
    show_default=True,
    type=str,
    help= "Select ECMWF api or CDS api."
)

def main(request_file:str, dates:str, week_filter:str ,parallel:bool, num_cores:int, mode:str="ecmwf"):
    """
    Python CLI tool to download data from ECMWF's mars web api. 
    To use the program you should privide a path to a json file with one or more 
    data request in valid MARS or CDS format in it i.e: 
    
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
    
    "request_config_2":{
        {'originating_centre': 'ukmo',
         'system': '600',
         'variable': 'total_precipitation',
         'year': '2021',
         'month': '09',
         'day': '01',
         'leadtime_hour': ['24', '48', '72', '96',
                           '120', '144', '168', '192',
                           '216', '240', '264', '288',
                           '312', '336', '360', '384',
                           '408', '432', '456', '480',
                           '504', '528', '552', '576',
                           '600', '624', '648', '672',
                           '696', '720', '744', '768',
                           '792', '816', '840', '864',
                           '888', '912', '936', '960',
                           '984', '1008', '1032', '1056',
                           '1080', '1104', '1128', '1152',
                           '1176', '1200', '1224', '1248',
                           '1272', '1296', '1320', '1344',
                           '1368', '1392', '1416', '1440',
                           '1464', '1488', '1512', '1536',
                           '1560', '1584', '1608', '1632',
                           '1656', '1680', '1704', '1728',
                            '1752', '1776', '1800', '1824',
                            '1848', '1872', '1896', '1920',
                            '1944', '1968', '1992', '2016',
                            '2040', '2064', '2088', '2112',
                            '2136', '2160', '2184', '2208',
                            '2232', '2256', '2280', '2304',
                            '2328', '2352', '2376', '2400',
                            '2424', '2448', '2472', '2496',
                            '2520', '2544', '2568', '2592',
                            '2616', '2640', '2664', '2688',
                            '2712', '2736', '2760', '2784', 
                            '2808', '2832', '2856', '2880', 
                            '2904', '2928', '2952', '2976', 
                            '3000', '3024', '3048', '3072', 
                            '3096', '3120', '3144', '3168', 
                            '3192', '3216', '3240', '3264', 
                            '3288', '3312', '3336', '3360', 
                            '3384', '3408', '3432', '3456', 
                            '3480', '3504', '3528', '3552', 
                            '3576', '3600', '3624', '3648', 
                            '3672', '3696', '3720', '3744', 
                            '3768', '3792', '3816', '3840', 
                            '3864', '3888', '3912', '3936', 
                            '3960', '3984', '4008', '4032', 
                            '4056', '4080', '4104', '4128', 
                            '4152', '4176', '4200', '4224', 
                            '4248', '4272', '4296', '4320', 
                            '4344', '4368', '4392', '4416', 
                            '4440', '4464', '4488', '4512', 
                            '4536', '4560', '4584', '4608', 
                            '4632', '4656', '4680', '4704',                            
                            '4728', '4752', '4776', '4800',                            
                            '4824', '4848', '4872', '4896', 
                            '4920', '4944', '4968', '4992', 
                            '5016', '5040', '5064', '5088', 
                            '5112', '5136', '5160'], 
                            'area': [15, -90, -60, 30],
                            'format': 'grib'} }

    more info on how to get a valid ECMWF data request on: https://www.ecmwf.int/en/forecasts/access-forecasts/ecmwf-web-api
    Usage example:
    downloader.py <path-to-request-file>

    """
    try:
        print("File downloader is being initialized.")
        tools = dataTools()

        config_dict = tools.load_config_file(request_file)
            
        if(mode == 'ecmwf'):
            retriever = ecmwfRetriever()    

            if (dates):
                click.echo(f'Date generator enabled. date_begin [{dates[0]}], date_end [{dates[1]}].')

                if (week_filter):
                        click.echo(f'week_filter enabled: {list(week_filter)}')          
                        date_list = tools.generate_dates(week_filter=list(week_filter))
                else:
                    date_list = tools.generate_dates(dates[0], dates[1])   

            else:
                dates = [None,None]
                date_list = None   

            if (parallel):            
                click.echo(f'Parallel processing enabled.') 

                if (num_cores):
                    click.echo(f'Using {str(num_cores)} cores.')
                    retriever.parallel_retrieve(config_dict, num_cores=num_cores, date_begin=dates[0], date_end=dates[1], date_list=date_list)
                
                else:
                    click.echo(f'Using {str(num_cores)} cores.')
                    retriever.parallel_retrieve(config_dict, date_begin=dates[0], date_end=dates[1], date_list=date_list, num_cores=2)

            else:
                click.echo(f'Sequential processing enabled.')
                retriever.sequential_retrieve(config_dict) 

        elif(mode == 'cds'):
            retriever = cdsRetriever()

            if (dates):
                click.echo(f'Date generator enabled. date_begin [{dates[0]}], date_end [{dates[1]}].') 
            else:
                dates = [None,None]

            if (parallel):            
                click.echo(f'Parallel processing enabled.') 

                if (num_cores):
                    click.echo(f'Using {str(num_cores)} cores.')
                    retriever.parallel_retrieve(config_dict, num_cores=num_cores, date_begin=dates[0], date_end=dates[1])
                
                else:
                    click.echo(f'Using 2 cores.')
                    retriever.parallel_retrieve(config_dict, num_cores=2, date_begin=dates[0], date_end=dates[1])

            else:
                click.echo(f'Sequential processing enabled.')
                retriever.sequential_retrieve(config_dict)

        else:
            raise TypeError(f'Mode should be eihter ecmwf or cds. got mode = {mode}. ')   

    except Exception as general_error:
        print(f'Error: {general_error}')


if __name__ == "__main__":
    main()



