from connect_to_db import connect_to_db

from pandas import DataFrame


def insert_to_db(df: DataFrame, table: str) -> None:
    """
    Insert data prepared in pandas Dataframe into the database.
    DB tables being affected are:
        - todays_orders - Updated orders that are completed daily
        - null_orders - Orders where delivery status/date is null.

    :param df: The dataframe containing the data to be inserted.
    :param table: A string representation of the table for data to be inserted into.

    :return: None
    """
    conn, engine = connect_to_db()

    index_labels = [
        'order_number',
        'toothbrush_type',
        'order_date',
        'customer_age',
        'order_quantity',
        'delivery_postcode',
        'billing_postcode',
        'is_first',
        'dispatch_status',
        'dispatch_date',
        'delivery_status',
        'delivery_date'
    ]

    for i, v in enumerate(index_labels):
        if v == 'is_first':
            continue
        converted_str = ' '.join(index_labels[i].split('_')).title()

        df.rename(columns={converted_str: index_labels[i]}, inplace=True)

    df_insert_attempt = df.to_sql(table, engine, if_exists='append', index=False)

    if df_insert_attempt:
        print('Number of rows affected:', df_insert_attempt)
    else:
        print(df_insert_attempt)
    return None
