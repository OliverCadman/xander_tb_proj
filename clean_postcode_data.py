from pandas import DataFrame


def clean_postcode(df: DataFrame) -> DataFrame:
    """
    Take in dirty postcode data and
    convert to a uniform format.

    Postcodes that are passed in vary in format, e.g:
        e25te
        SE4 1YR
        PR7%205RH
        AL3   4SB

    Clean this data such that the following postcode format is made uniform:
        AL34SB
    :param df - The dataframe to be modified
    :return: Modified dataframe
    """

    encoded_re = r'%\d{2}'
    whitespace_re = r'[ ]{1,}'

    d_address = df['delivery_postcode']
    b_address = df['billing_postcode']

    dp_encoded = d_address.str.contains(encoded_re)
    dp_whitespace = d_address.str.contains(whitespace_re)

    bp_encoded = b_address.str.contains(encoded_re)
    bp_whitespace = b_address.str.contains(whitespace_re)

    dp_islower = d_address.str.islower()
    bp_islower = b_address.str.islower()

    print(df['delivery_postcode'].str.split(r'\d+').str[0])

    # Modify columns based on given conditions
    df['delivery_postcode'] = (d_address.str.replace(encoded_re, '')).where(dp_encoded, df['delivery_postcode'])
    df['billing_postcode'] = (b_address.str.replace(encoded_re, '')).where(bp_encoded, df['billing_postcode'])

    df['delivery_postcode'] = (d_address.str.replace(whitespace_re, '')).where(dp_whitespace, df['delivery_postcode'])
    df['billing_postcode'] = (b_address.str.replace(whitespace_re, '')).where(bp_whitespace, df['billing_postcode'])

    df['delivery_postcode'] = (df['delivery_postcode'].str.upper()).where(dp_islower, df['delivery_postcode'])
    df['billing_postcode'] = (df['billing_postcode'].str.upper()).where(bp_islower, df['billing_postcode'])

    df['delivery_postcode_area'] = df['delivery_postcode'].str.split(r'\d+').str[0]
    df['billing_postcode_area'] = df['billing_postcode'].str.split(r'\d+').str[0]

    return df
