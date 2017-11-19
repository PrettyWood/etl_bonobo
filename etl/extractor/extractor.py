import logging

from etl.config_parser import DataSource
from etl.extractor.file_extractor import ExcelExtractor, CsvExtractor

logging.basicConfig(filename='etl.log',
                    level=logging.DEBUG,
                    format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')


class DataFrame:
    def __init__(self, domain, df):
        self.domain = domain
        self.df = df

    def __repr__(self):
        return f'DataFrame(domain="{self.domain}", columns={list(self.df.columns)}, ' \
               f'shape={self.df.shape})'


ETL_CONFIG = 'etl_config.yml'

EXTRACTOR_TYPE_MAPPING = {
    'excel': ExcelExtractor,
    'csv': CsvExtractor,
    # 'google_spreadsheet': SpreadsheetExtractor
}


class Extractor:
    def __init__(self):
        self.logger = logging.getLogger('Extractor')

    def extract(self, data_source: DataSource):
        self.logger.info(f'Processing {data_source}')
        df = EXTRACTOR_TYPE_MAPPING[data_source.type].extract(data_source)
        return DataFrame(data_source.domain, df)
