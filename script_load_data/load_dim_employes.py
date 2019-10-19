import pandas as pd
from settings import CONN,TRANSFORMED_FILES,SEP,ENCODING

def main():
    dim_employes = pd.read_csv(TRANSFORMED_FILES['dim_employes'],sep=SEP, encoding=ENCODING)
    dim_employes.to_sql(name='dim_employes',con=CONN, if_exists='append', index=False)


if __name__ == "__main__":
    main()