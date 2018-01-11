#%%
def csv_dtype_dict(filepath):
    import pandas as pd
    trip_col_names = pd.read_csv(filepath, nrows = 0)
    return dict(trip_col_names.dtypes)

csv_dtype_dict('/Users/corwinbell/prog_proj/TSRC_code/new_york/NY_2014_combined.csv')

#%%
import numpy as np
import pandas as pd
dft = pd.DataFrame(dict(A = np.random.rand(3),
B = 1,
C = 'foo',
D = pd.Timestamp('20010102'),
E = pd.Series([1.0]*3).astype('float32'),
F = False,
G = pd.Series([1]*3,dtype='int8')))

dft

dft_types = dict(dft.dtypes)
dft_types
dft_types = dft_types.replace( {'float64':'float','O':'object', 'int8':'int','<M8[ns]':'datetime'})

dtype_dict = dict(dft.dtypes)
dtype_dict['A']
#%% pandas to sql dtypes, this works well!
import sqlalchemy
import pandas as pd

def sqlcol(dfparam):

    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict

data_2 = pd.read_csv('data_2', index_col = 'Unnamed: 0')
outputdict = sqlcol(data_2)
outputdict

sqlcol(pd.read_csv('/Users/corwinbell/prog_proj/TSRC_code/new_york/NY_2014_mapped_combined.csv', nrows = 0))
column_errors.to_sql('load_errors',
                     push_conn,
                     if_exists = 'append',
                     index = False,
                     dtype = outputdict)
