import pandas as pd
from settings import CONN,TRANSFORMED_FILES,SEP,ENCODING

def main():
    dim_produits = pd.read_csv(TRANSFORMED_FILES['dim_produits'],sep=SEP, encoding=ENCODING)
    dim_produits.to_sql(name='dim_produits',con=CONN, if_exists='append', index=False)

if __name__ == "__main__":
    main()