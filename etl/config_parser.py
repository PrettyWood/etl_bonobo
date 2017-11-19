import os
from copy import copy

import funcy
from typing import List, Optional

ETL_PARAMS = [
    'input_domains',
    'function_name',
    'output_domains',
    'load'
]
DATA_SOURCE_DIR = os.path.abspath('data_sources')


class DataSource:
    def __init__(self, domain, type, **kwargs):
        self.domain = domain
        self.type = type
        self.file = None
        if 'file' in kwargs:
            self.file = os.path.join(DATA_SOURCE_DIR, kwargs.pop('file'))
        self.extras = kwargs

    def __repr__(self):
        file = f'"{self.file}"' if self.file is not None else '/'
        return f'Data source (domain: "{self.domain}", type: "{self.type}", file: {file})'


class ETLParams:
    def __init__(self, input_domains, *, function_name=None, output_domains=None,
                 load=True, chunksize=None):
        if isinstance(input_domains, str):
            input_domains = [input_domains]

        if output_domains is None and function_name is None:
            output_domains = copy(input_domains)
        elif isinstance(output_domains, str):
            output_domains = [output_domains]

        self.input_domains = input_domains
        self.function_name = function_name
        self.output_domains = output_domains
        self.load = load
        self.chunksize = chunksize

    def __repr__(self):
        rep = f'ETLParams(input_domains={self.input_domains}, function_name={self.function_name}, '\
              f'output_domains={self.output_domains}, load={self.load})'
        if self.chunksize is not None:
            rep += f' with chunks (chunk size: {self.chunksize})'
        return rep


class ETLConfig:
    def __init__(self, etl_config_dict):
        self.dict = etl_config_dict
        if 'DATA_SOURCES' not in self.dict:
            raise ETLConfigException('DATA_SOURCES missing in the etl config')
        self.data_source_domains = {d['domain'] for d in self.data_sources}
        self.pipeline_domains = set(funcy.flatten([d['input_domains']
                                                   for d in (self.pipeline or [])]))
        self.etl_data_sources = [{'input_domains': d['domain'], 'load': d.get('load', True)}
                                 for d in self.data_sources
                                 if d['domain'] in self.data_source_domains - self.pipeline_domains]
        self.etls = [ETLParams(**ds) for ds in self.etl_data_sources + (self.pipeline or [])]
        self.output_domains = [domain for etl in self.etls
                               for domain in (etl.output_domains or [])]
        self.input_domains = [domain for etl in self.etls for domain in etl.input_domains]
        self.functions = [etl.function_name for etl in self.etls
                          if etl.function_name is not None]

    def __repr__(self):
        return repr(self.dict)

    def __getattr__(self, item):
        return self.dict.get(item.upper(), None)

    def __getitem__(self, item):
        try:
            return self.etl_params([item])[0]
        except IndexError:
            raise ETLConfigException(f'Output domain {item} not defined in the etl config')

    def has_pipeline(self):
        return self.pipeline is not None

    def index(self, domain):
        indexes = self.mongo_indexes
        if isinstance(indexes, list):  # old behaviour
            return indexes
        elif isinstance(indexes, dict):
            return indexes.get('__DEFAULT__', []) + indexes.get(domain, [])

    def data_source(self, domain):
        found = [ds for ds in self.data_sources if ds['domain'] == domain]
        if not found:
            raise ETLConfigException(f'Domain {domain} not defined in the etl config data sources')
        return DataSource(**found[0])

    def etl_params(self, output_domains: Optional[list] = None) -> List[ETLParams]:
        """ etl params from output domain 
        In:  etl_config.etls(['aa', 'bbb'])
        Out: [ETLParams(input_domains=['lines1'],
                        function_name='out_lines1',
                        output_domains=['aa'],
                        load=True) with chunks (chunk size: 15),
              ETLParams(input_domains=['lines2', 'lines1'],
                        function_name='out_lines2',
                        output_domains=['aaa', 'bbb'],
                        load=True)]
        """
        if output_domains is None:
            return self.etls
        return [params for params in self.etls
                if any(x in (params.output_domains or []) for x in output_domains)]

    def params_from_input(self, domain: str) -> List[ETLParams]:
        """ get list of etl params from input domain """
        return [etl for etl in self.etls if domain in etl.input_domains]

    def params_from_function(self, func: str) -> List[ETLParams]:
        """ get list of etl params from function name """
        return [etl for etl in self.etls if etl.function_name == func]

    def get_output_domains(self, input_domains: list, function_names: list, output_domains: list):
        inp_func_domains = [etl.output_domains for etl in self.etls
                            if any(x in etl.input_domains for x in input_domains)
                            or etl.function_name in function_names]
        return [domain for domains in inp_func_domains for domain in domains] + output_domains


class ETLConfigException(Exception):
    pass
