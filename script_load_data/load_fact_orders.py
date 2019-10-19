import pandas as pd
from settings import CONN,TRANSFORMED_FILES,SEP,ENCODING


def main ():
    fact_orders = pd.read_csv(TRANSFORMED_FILES['fact_orders'],sep=SEP, encoding=ENCODING)
    fact_orders.to_sql(name='fact_orders',con=CONN, if_exists='append', index=False)

if __name__ == "__main__":
    main()