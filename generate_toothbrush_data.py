import pandas as pd
import numpy as np
import datetime as dt
import warnings
import os

from clean_postcode_data import clean_postcode

from api_client import APIClient

import time

from s3_client import S3Handler


def main():
    client = APIClient()
    warnings.filterwarnings('ignore')
    # set path or use working directory
    path = os.getcwd() + '/'

    # set if doing a full dump
    full_dump = False

    pd.set_option('display.max_colwidth', 60)
    pd.set_option('display.max_columns', 40)

    null_df = None
    if full_dump:
        # setting the size of the data
        n = np.random.choice(range(5000, 10000))
        start_date = pd.to_datetime('2021-01-01')

        end_date = pd.to_datetime(dt.date.today())
        max_id = 0
        df = generate_order_number(max_id, max_id + n, [])
        df = add_columns(df, start_date, end_date, n, path)
        df = add_delivery_columns(df, n)

        df = clean_postcode(df)

        full_filename = 'fulfilled_orders.csv'
        full_df = df.dropna()

        full_df.to_csv(f'{path}{full_filename}', index=False)

        # S3Handler().save_to_s3(full_filename, full_df)
        #
        client.post('todays_orders', df)
        client.post('full_orders', full_df)

    else:
        n = np.random.choice(range(500, 1000))
        start_date = pd.to_datetime(dt.date.today() - dt.timedelta(days=1))
        end_date = pd.to_datetime(dt.date.today())
        # reading in the previous data generated that wasn't delivered
        null_df, full_df, max_id = read_existing_data(path)
        # updating the delivery columns
        null_df = update_delivery_columns(null_df)
        # # Update columns in Database
        update_attempt = client.update('todays_orders', null_df)

        # adding order numbers to a list that already have data
        null_list = list(null_df['order_number'].str[3:].astype(int))
        # generating new data
        df = generate_order_number(max_id + 1, max_id + n, null_list)

        n = df.shape[0]
        df = add_columns(df, start_date, end_date, n, path)
        df = add_delivery_columns(df, n)
        # Clean dirty postcode data
        df = clean_postcode(df)
        client.post('todays_orders', df)

        df_copy = df.copy(deep=True)
        no_null_df_for_post = df_copy.dropna()
        client.post('full_orders', no_null_df_for_post)

        # adding the old data with new
        df = pd.concat([df, null_df], ignore_index=True)

        no_null_df = df.dropna()
        no_null_df.to_csv(f'{path}/no_null_df.csv', index=False)
        #
        full_df = full_df.append(no_null_df)
        full_filename = 'fulfilled_orders.csv'

        full_df.to_csv(f'{path}{full_filename}', index=False)

        # S3Handler().save_to_s3(full_filename, full_df)

    # # saving data to flat files
    file_name = 'order_data_today.csv'
    df.to_csv(f'{path}{file_name}', index=False)
    # S3Handler().save_to_s3(file_name, df)
    print('TODAYS ORDERS SAVED TO S3')

    # Insert completed data into DB (Table: 'todays_orders')
    null_df = df[df['delivery_date'].isnull()]
    null_df.to_csv(f'{path}null_orders.csv', index=False)

    client.post('null_orders', null_df)

    # S3Handler().save_to_s3('null_order_data.csv', null_df)

    print('NULL ORDERS SAVED TO S3')


def read_existing_data(path):
    print('READING...')
    print(os.listdir(path))
    max_id = 0
    null_df = None
    full_df = None
    # csv_files = S3Handler().read_from_s3()
    for filename in os.listdir(path):
        if filename.startswith("null") and filename.endswith('.csv'):
            null_df = pd.read_csv(path + filename)
            null_df['order_date'] = pd.to_datetime(null_df['order_date'], errors='coerce')
        elif filename.startswith('order_data') and filename.endswith('.csv'):
            df = pd.read_csv(path + filename)
            while max_id > int(df['order_number'].str[3:].max()):
                print(max_id, int(df['order_number'].str[3:].max()))
                continue
            else:
                max_id = int(df['order_number'].str[3:].max())
            print('FILE?????', filename)
            # Now delete the order_data CSV file to make way for fresh
            # S3Handler().delete_from_s3(file['filename'])
        elif filename.startswith('fulfilled') and filename.endswith('.csv'):
            print('READING FULL FILENAME?')
            full_df = pd.read_csv(path + filename)
    return null_df, full_df, max_id


def random_dates(start, end, n):
    start_u = start.value // 10 ** 9
    end_u = end.value // 10 ** 9
    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit='s')


def generate_order_number(l, n, null_list):
    lst = []
    start = l
    for i in range(l, n):
        if start in null_list:
            start += 1
        else:
            lst.append(''.join(['BRU{0:08}'.format(start)]))
            start += 1
    df = pd.DataFrame({'order_number': list(set(lst))})

    return df


def add_columns(df, start_date, end_date, n, path):
    # add two types of toothbrushes
    toothbrush_type = ['Toothbrush 2000', 'Toothbrush 4000']
    df['toothbrush_type'] = np.random.choice(toothbrush_type, size=n)

    tooth_1 = (df['toothbrush_type'] == 'Toothbrush 2000')
    tooth_2 = (df['toothbrush_type'] == 'Toothbrush 4000')

    len_tooth_1 = df[tooth_1].shape[0]
    len_tooth_2 = df[tooth_2].shape[0]

    # add random dates
    df['order_date'] = random_dates(start_date, end_date, n)
    df['order_date'] = pd.to_datetime(df['order_date'])

    # adding in insight re: time of order and toothbrush type
    time_1 = np.random.normal(11, 3.4, n)
    time_2 = np.random.normal(18, 4.5, n)

    df.loc[tooth_1, 'order_date'] = pd.to_datetime(df['order_date'] + pd.to_timedelta(time_1, unit='h'))
    df.loc[tooth_2, 'order_date'] = pd.to_datetime(df['order_date'] + pd.to_timedelta(time_2, unit='h'))

    # adding in insight: re age of orderer and toothbrush type
    age_1 = np.random.normal(75, 11, len_tooth_1)
    age_2 = np.random.normal(26, 9, len_tooth_2)

    df.loc[tooth_1, 'customer_age'] = age_1
    df.loc[tooth_2, 'customer_age'] = age_2

    df['customer_age'] = df['customer_age'].astype(int)

    # adding quantity
    df['order_quantity'] = np.random.choice(range(1, 10), n)

    postcodes = pd.read_csv(f'{path}/open_postcode_geo_min2.csv', header=None, usecols=['postcode'], names=['postcode'])

    # randomly choosing postcodes
    df['delivery_postcode'] = list(postcodes['postcode'].sample(n))
    # setting the billing postcode as the delivery postcode
    df['billing_postcode'] = df[['delivery_postcode']]

    # randomly picking the number of records where the billing and delivery postcode are different
    postcode_split = np.random.choice(range(1, int(n / 2)), 1)[0]
    # randomly picking a different billing postcode
    df.loc[:postcode_split - 1, 'billing_postcode'] = list(postcodes['postcode'].sample(postcode_split))

    # dirty the postcode data
    lower = np.random.choice(range(1, int(n / 3)), 1)[0]
    upper = np.random.choice(range(int(n / 3), n), 1)[0]
    df.loc[lower:upper, 'delivery_postcode'] = df['delivery_postcode'].str.replace(' ', '').str.lower()
    df.loc[lower:upper, 'billing_postcode'] = df['billing_postcode'].str.replace(' ', '').str.lower()
    df.loc[:lower, 'delivery_postcode'] = df.loc[:lower, 'delivery_postcode'].str.replace(' ', '%20')
    df.loc[upper:, 'billing_postcode'] = df.loc[upper:, 'billing_postcode'].str.replace(' ', '   ')

    df.loc[:, 'is_first'] = True
    return df


def add_delivery_columns(df, n):
    days_ago = dt.date.today() - dt.timedelta(days=3)

    # add dispatch status
    dispatch_status = ['Order Received', 'Order Confirmed', 'Dispatched']
    df['dispatch_status'] = np.random.choice(dispatch_status, size=n)

    # all orders have been dispatched for first run
    df.loc[(df['order_date'].dt.date < days_ago), 'dispatch_status'] = 'Dispatched'

    # generate time intervals
    order_received = np.random.normal(0.2, 0.01, n)
    order_confirmed = np.random.normal(0.9, 0.2, n)
    order_dispatched = np.random.normal(6, 0.5, n)

    # generate dispatch time
    df.loc[df['dispatch_status'] == 'order_received', 'dispatch_date'] = pd.to_datetime(
        df['order_date'] + pd.to_timedelta(order_received, unit='h'))
    df.loc[df['dispatch_status'] == 'Order Confirmed', 'dispatch_date'] = pd.to_datetime(
        df['order_date'] + pd.to_timedelta(order_received + order_confirmed, unit='h'))
    df.loc[df['dispatch_status'] == 'Dispatched', 'dispatch_date'] = pd.to_datetime(
        df['order_date'] + pd.to_timedelta(order_received + order_confirmed + order_dispatched, unit='h'))

    # add delivery status to generate insight re: unsuccessful deliveries before 4am
    delivery_status = ['In Transit', 'Delivered', 'Unsuccessful']

    dispatch_mask_1 = (df['dispatch_status'] == 'Dispatched') & (df['dispatch_date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'delivery_status'] = np.random.choice(delivery_status, p=[0.4, 0.2, 0.4])

    dispatch_mask_2 = (df['dispatch_status'] == 'Dispatched') & (df['dispatch_date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'delivery_status'] = np.random.choice(delivery_status, p=[0.3, 0.69, 0.01])

    # forcing all old orders to have some delivery data
    delivery_status = ['Delivered', 'Unsuccessful']
    dispatch_mask_1 = (df['order_date'].dt.date < days_ago) & (df['dispatch_date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'delivery_status'] = np.random.choice(delivery_status, p=[0.8, 0.2])

    dispatch_mask_2 = (df['order_date'].dt.date < days_ago) & (df['dispatch_date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'delivery_status'] = np.random.choice(delivery_status, p=[0.99, 0.01])

    # generate time intervals
    in_transit = np.random.normal(1, 0.2, n)
    delivered = np.random.normal(26, 4, n)
    unsuccessful = np.random.normal(26, 8, n)

    # generate delivery time
    df.loc[df['delivery_status'] == 'In Transit', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit, unit='h'))
    df.loc[df['delivery_status'] == 'Delivered', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit + delivered, unit='h'))
    df.loc[df['delivery_status'] == 'Unsuccessful', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit + unsuccessful, unit='h'))

    return df


def update_delivery_columns(df):
    # orders that weren't dispatched in the first generation, are updated to dispatch
    df.loc[(df['dispatch_status'] != 'Dispatched'), 'dispatch_status'] = 'Dispatched'

    n = df.shape[0]
    # generate time intervals
    order_received = np.random.normal(0.2, 0.01, n)
    order_confirmed = np.random.normal(0.9, 0.2, n)
    order_dispatched = np.random.normal(6, 0.5, n)

    # add dispatch time
    df.loc[df['dispatch_status'] == 'Dispatched', 'dispatch_date'] = pd.to_datetime(
        df['order_date'] + pd.to_timedelta(order_received + order_confirmed + order_dispatched, unit='h'))

    delivery_status_transit = ['Delivered', 'Unsuccessful']

    # update delivery status for old data
    null_dispatch_mask_1 = (df['delivery_status'] == 'In Transit') & (df['dispatch_date'].dt.hour <= 4)
    df.loc[null_dispatch_mask_1, 'delivery_status'] = np.random.choice(delivery_status_transit, p=[0.8, 0.2])
    null_dispatch_mask_2 = (df['delivery_status'] == 'In Transit') & (df['dispatch_date'].dt.hour > 4)
    df.loc[null_dispatch_mask_2, 'delivery_status'] = np.random.choice(delivery_status_transit, p=[0.99, 0.01])

    # add delivery status to generate insight re: unsuccessful deliveries before 4am
    delivery_status = ['In Transit', 'Delivered', 'Unsuccessful']

    dispatch_mask_1 = (df['dispatch_status'] == 'Dispatched') & (df['dispatch_date'].dt.hour <= 4)
    df.loc[dispatch_mask_1, 'delivery_status'] = np.random.choice(delivery_status, p=[0.4, 0.2, 0.4])

    dispatch_mask_2 = (df['dispatch_status'] == 'Dispatched') & (df['dispatch_date'].dt.hour > 4)
    df.loc[dispatch_mask_2, 'delivery_status'] = np.random.choice(delivery_status, p=[0.3, 0.69, 0.01])

    # generate time intervals
    in_transit = np.random.normal(1, 0.2, n)
    delivered = np.random.normal(26, 4, n)
    unsuccessful = np.random.normal(26, 8, n)

    # generate delivery time
    df.loc[df['delivery_status'] == 'In Transit', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit, unit='h'))
    df.loc[df['delivery_status'] == 'Delivered', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit + delivered, unit='h'))
    df.loc[df['delivery_status'] == 'Unsuccessful', 'delivery_date'] = pd.to_datetime(
        df['dispatch_date'] + pd.to_timedelta(in_transit + unsuccessful, unit='h'))
    return df


if __name__ == '__main__':
    t1 = time.perf_counter()
    main()
    t2 = time.perf_counter()


    print(f'Execution took {t2 - t1} seconds to complete.')