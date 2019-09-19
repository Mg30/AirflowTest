import os
from sqlalchemy import create_engine

CONN = create_engine('postgresql+psycopg2://postgres:USER@localhost:5432/DATABASE')
BASE_DIR = '/source'
ENCODING = 'latin1'
SEP = ';'
SOURCE_FILES = {
    'produits': os.path.join(BASE_DIR, 'Northwind_Products.csv'),
    'clients': os.path.join(BASE_DIR, 'Northwind_Customers.csv'),
    'orders': os.path.join(BASE_DIR, 'Northwind_Orders.csv'),
    'orders_details':os.path.join(BASE_DIR, 'Northwind_OrderDetails.csv'),
    'categories': os.path.join(BASE_DIR, 'Northwind_Categories.csv'),
    'employes': os.path.join(BASE_DIR, 'Northwind_Employees.csv'),
}

TRANSFORMED_FILES_DIR = '/mnt/c/Users/Matt/Desktop/transformed'

TRANSFORMED_FILES = {
    'dim_produits': os.path.join(TRANSFORMED_FILES_DIR, 'dim_produits.csv'),
    'dim_clients': os.path.join(TRANSFORMED_FILES_DIR, 'dim_clients.csv'),
    'dim_employes': os.path.join(TRANSFORMED_FILES_DIR, 'dim_employes.csv'),
    'fact_orders': os.path.join(TRANSFORMED_FILES_DIR, 'fact_orders.csv'),
}

CREATE_DIM_PRODUITS = '''
CREATE TABLE IF NOT EXISTS DIM_PRODUITS
(ProductID INTEGER PRIMARY KEY,
ProductName TEXT,
UnitPrice REAL,
CategoryName TEXT,
Description TEXT
)

'''

CREATE_DIM_CLIENTS = '''
CREATE TABLE IF NOT EXISTS DIM_CLIENTS
( CustomerID TEXT PRIMARY KEY,
CompanyName TEXT,
ContactName TEXT,
ContactTitle TEXT,
Address TEXT,
City TEXT,
Region TEXT,
PostalCode TEXT,
Country TEXT
)
'''

CREATE_DIM_EMPLOYES = '''
CREATE TABLE IF NOT EXISTS DIM_EMPLOYES
(EmployeeID INTEGER PRIMARY KEY,
LastName TEXT,
FirstName TEXT,
Title TEXT,
TitleOfCourtesy TEXT,
BirthDate TEXT,
HireDate TEXT,
Address TEXT,
City TEXT, 
Region TEXT,
PostalCode TEXT,
Country TEXT,
Age INTEGER ,
TrancheAge TEXT
)
'''

CREATE_FACT_ORDERS = '''
CREATE TABLE IF NOT EXISTS FACT_ORDERS
(
OrderID INTEGER,
CustomerID TEXT,
UnitPrice REAL,
Quantity INTEGER,
OrderDate TEXT,
EmployeeID INTEGER,
ProductID INTEGER,
FOREIGN KEY (CustomerID) REFERENCES DIM_CLIENTS (CustomerID) 
 ON DELETE CASCADE ON UPDATE NO ACTION,
 FOREIGN KEY (EmployeeID) REFERENCES DIM_EMPLOYES (EmployeeID) 
 ON DELETE CASCADE ON UPDATE NO ACTION,
 FOREIGN KEY (ProductID) REFERENCES DIM_PRODUITS (ProductID) 
 ON DELETE CASCADE ON UPDATE NO ACTION

)
'''
CREATE_STATEMENTS = [CREATE_DIM_EMPLOYES,CREATE_DIM_CLIENTS,CREATE_DIM_CLIENTS,CREATE_DIM_PRODUITS,CREATE_FACT_ORDERS]