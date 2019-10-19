import pandas as pd
from settings import CONN, TRANSFORMED_FILES, SEP, ENCODING


def main():
    dim_clients = pd.read_csv(
        TRANSFORMED_FILES['dim_clients'], sep=SEP, encoding=ENCODING)
    dim_clients.to_sql(name='dim_clients', con=CONN, if_exists='append', index=False)

if __name__ == "__main__":
    main()