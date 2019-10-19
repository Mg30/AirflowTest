import pandas as pd
import pendulum
from settings import SOURCE_FILES, SEP, ENCODING,TRANSFORMED_FILES

pendulum.set_locale('fr')


def calcul_age(date):
    date = date.split(' ')[0]
    maxDate = pendulum.datetime(1998, 1, 1)
    date_pendulum = pendulum.parse(date)
    return maxDate.diff(date_pendulum).in_years()


def tranche_age(age):
    if age < 35:
        return 'Moins de 35 ans'
    elif 35 <= age <= 50:
        return '35 - 50 ans'
    else:
        return 'Plus de 50 ans'


def main():
    df_employes = pd.read_csv(SOURCE_FILES['employes'],sep=SEP, encoding=ENCODING)

    dim_employes = df_employes.drop(
        ['HomePhone', 'Extension', 'Photo', 'Notes', 'ReportsTo', 'PhotoPath'], axis=1)
    
    dim_employes["Age"] = dim_employes["BirthDate"].apply(calcul_age)
    dim_employes["TrancheAge"] = dim_employes["Age"].apply(tranche_age)
    dim_employes = dim_employes.fillna('')
    dim_employes.columns = [x.lower() for x in dim_employes.columns]
    dim_employes.to_csv(TRANSFORMED_FILES['dim_employes'],sep=SEP,index=False)


if __name__ == "__main__":
    main()
