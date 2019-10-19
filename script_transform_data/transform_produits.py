import pandas as pd
from settings import SOURCE_FILES, SEP, ENCODING,TRANSFORMED_FILES


def main():

    df_produits = pd.read_csv(
        SOURCE_FILES['produits'], sep=SEP, encoding=ENCODING)
    df_categories = pd.read_csv(SOURCE_FILES['categories'],sep=SEP,encoding=ENCODING)
   

    dim_produit = df_produits.merge(
        df_categories, how='inner', on='CategoryID')
    dim_produit = dim_produit.fillna('')
    dim_produit = dim_produit[['ProductID', 'ProductName',
                               'UnitPrice', 'CategoryName', 'Description']]
    dim_produit.columns = [x.lower() for x in dim_produit.columns]
    dim_produit['unitprice'] = dim_produit['unitprice'].str.replace(',','.').astype(float)
    dim_produit.to_csv(TRANSFORMED_FILES['dim_produits'],sep=SEP,index=False)


if __name__ == "__main__":
    main()
