def csv_to_pg(csv_filepath, chunksize, conf_filepath, table_name, database):
    """takes a csv table, reads as pandas dataframe in chunks,
    coerces all values to string, and appends to postgres database
    csv_filepath: complete path of csv
    conf_filepath: path to configuration json file
    table name: desired sql table name
    database: name of destination database
    """
    import pandas as pd
    from sqlalchemy import create_engine
    import io
    import json

    with open(conf_filepath) as f:
        conf = json.load(f)
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conf['user'],conf['passw'],conf['host'], conf['port'], database))
    #engine = create_engine('postgresql://' + conf['user'] + ':' + conf['passw'] + '@' + conf['host'] + ':' conf['port'] + '/' + database)

    reader = pd.read_csv('{}'.format(csv_filepath), index_col = 'Unnamed: 0' , chunksize = chunksize)
    chunk_count = 0
    for chunk in reader:
        chunk.astype(str, copy = False)
        chunk.to_sql('{}'.format(table_name), engine, if_exists = 'append')
        chunk_count += 1
        print(chunk_count)

csv_to_pg('NY_2014_mapped_combined.csv',500000,'brower_local_config.json','ny2014','postgres')
