import pandas as pd


def clean_postcode(df: pd.DataFrame) -> pd.DataFrame:

    d_postcode = df['Delivery Postcode']
    b_postcode = df['Billing Postcode']

    encoded_re = r'%\d{2}'
    whitespace_re = r'[ ]{1,}'

    encoded = d_postcode.str.contains(encoded_re)
    whitespace = b_postcode.str.contains(whitespace_re)

    dp_islower = d_postcode.str.islower()
    bp_islower = b_postcode.str.islower()

    # Remove encoding
    df['Delivery Postcode'] = df['Delivery Postcode'].str.replace(r'%\d{2}', '').where(encoded, df['Delivery Postcode'])
    df['Billing Postcode'] = df['Billing Postcode'].str.replace(r'[ ]{1,}', '').where(encoded, df['Billing Postcode'])

    # Remove whitespace
    df['Delivery Postcode'] = df['Delivery Postcode'].str.replace(r'[ ]{1,}', '').where(
        whitespace, df['Delivery Postcode'])
    df['Billing Postcode'] = df['Billing Postcode'].str.replace(r'[ ]{1,}', '').where(
        whitespace, df['Billing Postcode'])

    # Convert lowercase to upper
    df['Delivery Postcode'] = df['Delivery Postcode'].str.upper().where(dp_islower, df['Delivery Postcode'])
    df['Billing Postcode'] = df['Billing Postcode'].str.upper().where(bp_islower, df['Billing Postcode'])

    return df
