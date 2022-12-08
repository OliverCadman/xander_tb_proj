from connect_to_db import connect_to_db

import pandas as pd

from typing import Union


def get_postcodes(n: str) -> Union[pd.DataFrame, None]:
    """
    Retrieve live postcodes from AWS RDS Database
    Serves as a solution to pulling from CSV file,
    which would be too large to fit in Lambda Function.
    params:
        n - Randomly generated number. Used to limit the amount of returned rows.
    :return:
    """

    conn, _ = connect_to_db()

    if not conn:
        return None

    else:
        query = """
            SELECT postcode FROM postcodes
            ORDER BY RANDOM()
            LIMIT {};
        """.format(n)

        postcodes = pd.read_sql(query, conn)

        return postcodes
