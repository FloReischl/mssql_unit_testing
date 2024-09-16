import pandas as pd
from io import StringIO
from .dbcmd import DbCmd
from typing import Iterable

class DataLoader:
    """
    Helper class to load data into a SQL Server database; mainly by using methods defined by the (PyScripter).
    """
    def load_df(self, callback: callable, df: pd.DataFrame):
        """
        Loads a (DataFrame) into the databasse by utilizing the given callback method.

        Parameters
        ----------
        callback    : callable
                      The callback method to be called for each row of the (DataFrame).
        df          : DataFrame
                      The data frame to be loaded into the database.
        """
        data = dict()
        for i,row in df.iterrows():
            for c in df.columns:
                data[c] = row[c]
            
            callback(**data)

    def load_csv_str(self, callback: callable, csv: str, separator: str = ',', names:Iterable=None, header: int = None, dtype: dict=None, date_format=None):
        """
        Load a CSV string into the databasse by utilizing the given callback method.

        Parameters
        ----------
        callback    : callable
                      The callback method to be called for each line of the CSV string.
        csv         : str
                      The string containing the CSV data to be loaded.
        separator   : str
                      The separator within the CSV string
        names:      : Iterable
                      The sequence of column names to be loaded. If not provided, it will be taken
                      from the given input CSV (headedr=0).
        header      : int
                      The line where the header can be read from the input CSV. If None, the given
                      names will be used.
        dtypes:     : dict
                      Data type(s) to apply to either the whole dataset or individual columns.
        date_format : str
                      str or dict of column -> format, default ``None``
        """
        buffer = StringIO(csv)
        return self.load_csv_file(callback, buffer, separator, names, header, dtype, date_format)

    def load_csv_file(self, callback: callable, filepath_or_buffer, separator: str = ',', names=None, header: int = None, dtype: dict=None, date_format=None):
        """
        Load a CSV file or buffer into the databasse by utilizing the given callback method.

        Parameters
        ----------
        callback    : callable
                      The callback method to be called for each line of the CSV string.
        filepath_or_buffer : str or IOBase
                      File or buffer to read the CSV from.
        separator   : str
                      The separator within the CSV string
        names:      : Iterable
                      The sequence of column names to be loaded. If not provided, it will be taken
                      from the given input CSV (headedr=0).
        header      : int
                      The line where the header can be read from the input CSV. If None, the given
                      names will be used.
        dtypes:     : dict
                      Data type(s) to apply to either the whole dataset or individual columns.
        date_format : str
                      str or dict of column -> format, default ``None``
        """
        df = pd.read_csv(filepath_or_buffer, sep=separator, names=names, header=header, dtype=dtype, date_format=date_format)
        self.load_df(callback=callback, df=df)
