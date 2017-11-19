import logging
import os

import bonobo
import yaml

from etl.config_parser import ETLConfig, ETLParams
from etl.extractor.extractor import Extractor, DataFrame

ETL_CONFIG = 'config/etl_config.yml'
OUTPUT_DIR = 'outputs'

logging.basicConfig(filename='etl.log',
                    level=logging.DEBUG,
                    format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')


class ETL:
    def __init__(self, etls=None):
        with open(ETL_CONFIG) as f:
            etl_config = yaml.load(f)
        self.etl_config = ETLConfig(etl_config)
        self.etls = etls or self.etl_config.etls
        self.extractor = Extractor()
        self.transformer = None
        self.loader = None
        self.logger = logging.getLogger('ETL')

    def extract(self):
        """
        yield the list of etls (ETLParams) with the 
        extracted data sourc added in input_domains
        """
        for etl in self.etls:
            self.logger.info(f'Processing {etl}')
            etl.input_domains = [self.extractor.extract(self.etl_config.data_source(domain))
                                 for domain in etl.input_domains]
            yield etl

    def transform(self, etl: ETLParams):
        """
        input : ETLParams(input_domains=[DataFrame(domain="lines1", df=...), ...],
                          function_name=out_lines1,
                          output_domains=['aa'],
                          load=True)
        Returns:
            generator of DataFrames (domain, df)
        """
        self.logger.info(f'Transforming {etl}')
        inputs = {x.domain: x.df for x in etl.input_domains}

        if etl.function_name is not None:
            from config import augment
            func = getattr(augment, etl.function_name)
            if not callable(func):
                raise Exception(f'{etl.function_name} exists but is not a function')
            outputs = func(inputs)
        else:
            outputs = inputs
        if etl.load:
            yield from (DataFrame(*t) for t in outputs.items())

    def load(self, dataframe: DataFrame):
        """ Load the dataframe.df in the collection dataframe.domain """
        self.logger.info(f'Loading "{dataframe.domain}"')
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        output_path = os.path.join(OUTPUT_DIR, f'{dataframe.domain}.csv')
        dataframe.df.to_csv(output_path, index=False)


if __name__ == '__main__':
    from bonobo.commands.run import get_default_services

    etl = ETL()
    graph = bonobo.Graph(
        etl.extract,
        etl.transform,
        etl.load,
    )
    bonobo.run(graph, services=get_default_services(__file__))
