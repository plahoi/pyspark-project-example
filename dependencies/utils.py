''' Utils 
'''

def test_persist_data(data):
    data.limit(2).write.csv('jobs/test_apolyakov/mycsv.csv')

def persist_result(data, destination_table):
    ''' Partition result data and write it to the destination_table table
    Arguments:
        data (DataFrame): Dataframe to save
        destination_table (string): Table to write result data
    '''
    data.repartition('day').write.format('orc').mode(
        'overwrite').insertInto(destination_table, overwrite=True)

def check_is_date_format(string):
    import datetime
    ''' Checks whether "s" is in date format YYYY-MM-DD or not
    Arguments:
        string
    Returns:
        string (str)
    Raises:
        ValueError
    '''
    try:
        string = str(string)
        datetime.datetime.strptime(string, '%Y-%m-%d')
    except ValueError:
        raise ValueError(
            'Wrong string format. Should be: YYYY-MM-DD, given: "{1}"'.format(string))

    return string

def generate_spark_tables(src, spark):
    ''' Generates spark table instances from given dict
    Arguments:
        src (dict): Dictionary {"table_alias": "schema_name.table_name"}
        spark (SparkContext): Given spark context
    Returns:
        Dict {"table_alias": spark.table("schema_name.table_name")}
    '''
    return {src_table_key: spark.table(src_table_name)
           for src_table_key, src_table_name in src.items()}

def week_day(date):
    ''' Returns the number of week of given string/date/datetime
    monday = 1

    Arguments:
        date (string/date/datetime): 
    
    Retruns:
        day of week (int)
    '''
    import datetime
    if isinstance(date, str):
        date = date[:10]
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    if not isinstance(date, datetime.datetime) and not isinstance(date, datetime.date):
        return None
    return date.weekday() + 1