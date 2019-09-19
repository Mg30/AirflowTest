import pandas as pd
from settings import SOURCE_FILES, SEP, TRANSFORMED_FILES, ENCODING


def main():
    df_clients = pd.read_csv(
        SOURCE_FILES['clients'], sep=SEP, encoding=ENCODING)

    dim_clients = df_clients.loc[:, :'Country'].fillna('')


    dim_clients.columns = dim_clients.columns.str.lower()
    dim_clients.to_csv(TRANSFORMED_FILES['dim_clients'], sep=SEP, index=False)


if __name__ == "__main__":
    main()
