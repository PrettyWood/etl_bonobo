import logging
import pandas as pd
import sys

from etl.config_parser import DataSource

logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG,
                    format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')


class FileExtractor:
    @classmethod
    def extract(cls, data_source):
        logging.getLogger(__name__).info(f'[{cls.__name__}] Extracting {data_source}')


class ExcelExtractor(FileExtractor):
    @classmethod
    def extract(cls, data_source: DataSource):
        super().extract(data_source)
        return pd.read_excel(data_source.file, **data_source.extras)


class CsvExtractor(FileExtractor):
    @classmethod
    def extract(cls, data_source: DataSource):
        super().extract(data_source)
        return pd.read_csv(data_source.file, **data_source.extras)
