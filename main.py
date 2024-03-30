import pandas as pd
from connection import get_connection
import os

from fill_df import fill_df
from constants import PATH_FILLED_DF

connection = get_connection()

df = pd.read_sql(sql='select * from ane_base.parser_app_pricesraw',
                 con=connection)
filled_df = fill_df(df)
filled_df.to_csv(os.path.join(PATH_FILLED_DF, 'filled.csv'))

print(filled_df.head())
