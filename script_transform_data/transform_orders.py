import pandas as pd
from settings import SOURCE_FILES, SEP, ENCODING,TRANSFORMED_FILES


def main():
    df_orders = pd.read_csv(SOURCE_FILES['orders'], sep=SEP, encoding=ENCODING)
    df_orders.columns = [x.lower() for x in df_orders.columns]
    df_orders_details = pd.read_csv(
        SOURCE_FILES['orders_details'], sep=SEP, encoding=ENCODING)
    df_orders_details.columns = [x.lower() for x in df_orders_details.columns]

    fact_orders = df_orders.merge(
        df_orders_details, how='inner', on='orderid').fillna('')

    fact_orders = fact_orders[['orderid', 'customerid', 'employeeid',
                               'productid', 'unitprice', 'quantity', 'orderdate']]
    fact_orders['orderdate'] = pd.to_datetime(fact_orders['orderdate'])
    fact_orders['unitprice'] = fact_orders['unitprice'].str.replace(',','.').astype(float)
    fact_orders.to_csv(TRANSFORMED_FILES['fact_orders'],sep=SEP,index=False)


if __name__ == "__main__":
    main()
