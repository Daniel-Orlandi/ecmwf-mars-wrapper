import click
from src.model_data_retriever.ecmwf_request import ECMWF_retriever

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

def main(request_file:str, dates:str, week_filter:str ,parallel:bool, num_cores:int):
    """
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

    """
    print("ECMWF File downloader is being initialized.")

    if (dates):
        click.echo(f'Date generator enabled. date_begin [{dates[0]}], date_end [{dates[1]}].')
        retriever = ECMWF_retriever(dates[0], dates[1])

        if (week_filter):
            click.echo(f'week_filter enabled: {list(week_filter)}')          
            retriever.date_list = retriever.generate_dates(week_filter=list(week_filter))
    
    else:
        retriever = ECMWF_retriever()    
    
    config_dict = retriever.load_config_file(request_file)

    if (parallel):            
        click.echo(f'Parallel processing enabled.') 
        if (num_cores):
            click.echo(f'Using {str(num_cores)} cores.')
            retriever.parallel_retrieve(config_dict, num_cores=num_cores)
        
        else:
            click.echo(f'Using {str(num_cores)} cores.')
            retriever.parallel_retrieve(config_dict, num_cores=2)

    else:
        click.echo(f'Sequential processing enabled.')
        retriever.sequential_retrieve(config_dict)    
    


if __name__ == "__main__":
    main()



