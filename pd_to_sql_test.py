%%timeit
import psycopg2
import json
import pandas as pd
import io

with open('/Users/corwinbell/prog_proj/TSRC_code/postgres/corwinbell_config.json') as f:
    conf = json.load(f)
conn_str = "host={} dbname={} user={} password={}".format(conf['host'], conf['database'], conf['user'], conf['passw'])

conn = psycopg2.connect(conn_str)

# df = pd.read_sql('select * from table_name', con=conn)

f = io.StringIO()
pd.DataFrame({'a':[1,2], 'b':[3,4]}).to_csv(f, index=False, header=False)  # removed header
f.seek(0)  # move position to beginning of file before reading
cursor = conn.cursor()
cursor.execute('create table bbbb (a int, b int);COMMIT; ')
cursor.copy_from(f, 'bbbb', columns=('a', 'b'), sep=',')
cursor.execute("select * from bbbb;")
a = cursor.fetchall()
print(a)
cursor.close()
#%%
%%timeit
import pandas as pd
from sqlalchemy import create_engine

#engine = create_engine('postgresql://corwinbell:b1g_data!@localhost:5432/first_db')

engine = create_engine('postgresql://corwin_test:b1g_data@localhost:5432/first_db')

data_2 = pd.DataFrame({'a':[1,2], 'b':[3,4]})
data_2.to_csv('data_2')
data_2.to_sql('data_2', engine)

#%%
%%timeit
import pandas as pd
data_2 = pd.read_csv('data_2', index_col = 'Unnamed: 0')
data_2


#%%
def csv_to_pg(csv_filepath, chunksize, conf_filepath, table_name):
    """takes a csv table, reads as pandas dataframe in chunks,
    coerces all values to string, and appends to postgres database
    csv_filepath: complete path of csv
    conf_filepath: path to configuration json file
    table name: desired sql table name
    """
    import pandas as pd
    from sqlalchemy import create_engine
    import io
    import json

    with open('/Users/corwinbell/prog_proj/TSRC_code/postgres/corwin_test_config.json') as f:
        conf = json.load(f)
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conf['user'],conf['passw'],conf['host'], conf['port'], conf['database']))

    reader = pd.read_csv('{}'.format(csv_filepath), index_col = 'Unnamed: 0' , chunksize = chunksize)
    chunk_count = 0
    for chunk in reader:
        chunk.astype(str, copy = False)
        chunk.to_sql('{}'.format(table_name), engine, if_exists = 'append')
        chunk_count += 1
        print(chunk_count)
        break
%timeit csv_to_pg('/Users/corwinbell/prog_proj/TSRC_code/new_york/NY_2014_mapped_combined.csv',200000,'corwin_test_config.json','ny2014')

%timeit csv_to_pg('/Users/corwinbell/prog_proj/TSRC_code/new_york/NY_2014_mapped_combined.csv',1000000,'corwin_test_config.json','ny2014')

# %%
%%timeit
import psycopg2
import json
import pandas as pd
import io

with open('/Users/corwinbell/prog_proj/TSRC_code/postgres/corwin_test_config.json') as f:
    conf = json.load(f)
conn_str = "host={} dbname={} user={} password={}".format(conf['host'], conf['database'], conf['user'], conf['passw'])

conn = psycopg2.connect(conn_str)

# df = pd.read_sql('select * from table_name', con=conn)

f = io.StringIO()
pd.read_csv('{}'.format(csv_filepath), index_col = 'Unnamed: 0' , chunksize = chunksize)  # removed header
f.seek(0)  # move position to beginning of file before reading
cursor = conn.cursor()
cursor.execute('create table ny2014 (a int, b int);COMMIT; ')
cursor.copy_from(f, 'bbbb', columns=('a', 'b'), sep=',')
cursor.execute("select * from bbbb;")
a = cursor.fetchall()
print(a)
cursor.close()
