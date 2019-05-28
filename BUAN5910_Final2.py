#Mark Russeff
#Final, Problem #2

import pandas as pd
import sqlite3 as sql
from sqlite3 import Error
import sys
import datetime
import os.path
import collections
import matplotlib.pyplot as plt


class Database:
    @staticmethod
    def GetConnection(dbName):
        try:
            conn = sql.connect(dbName)
            return conn
        except Error as e:
            print(e)
    
    @staticmethod
    def CloseConnection(conn):
        try:
            conn.close()
        except Error as e:
            print(e)

class Customer: 
    def __init__ (self, id, name, sex, age):
        self.__id = id
        self.__name = name
        self.__sex = sex
        self.__age = age

    
    @property
    def ID (self):
        return self.__id

    @ID.setter
    def ID (self, id):
        self.__id = id   
    
    @property
    def Name (self):
        return self.__name

    @Name.setter
    def Name (self, name):
        self.__name = name


    @property
    def Sex (self):
        return self.__sex

    @Sex.setter
    def Sex (self, sex):
        self.__sex = sex


    @property
    def Age (self):
        return self.__age

    @Age.setter
    def Age (self, age):
        self.__age = age

    def __str__ (self):
        return "({0},{1},{2},{3})".format(self.__id, self.__name, self.__sex, self.__age)


class CustomerDataManager:

    @staticmethod
    def CreateNewTable(conn):

        CreateCustomerTableSQL =    """
                            CREATE TABLE IF NOT EXISTS Customer (
                                id integer PRIMARY KEY,
                                name text,
                                sex text,
                                age int
                            );
                            """
        try:
            cursor = conn.cursor()
            cursor.execute(CreateCustomerTableSQL)
            conn.commit() 
        except Error as e:
            print(e)

    @staticmethod
    def LoadAllData(data, conn):

        customers = pd.read_csv(data)

        try:
            cursor = conn.cursor()
            #Clears old values from the table.
            cursor.execute("DELETE FROM Customer")

            for row in customers.itertuples():
                entry = (row.ID, row.name, row.sex, row.age)
    
                sqlBase =  """INSERT INTO Customer (id, name, sex, age)
                VALUES (?,?,?,?)"""

                cursor.execute(sqlBase, entry)
                
            conn.commit() 
        except Error as e:
            print(e)

    @staticmethod
    def AddCustomer(newCustomer, conn):
        #Adds customer but first checks if the id for the customer already exists.
        sqlBase = "SELECT * FROM Customer WHERE id = " + str(newCustomer.ID)

        sqlBaseNew =  """INSERT INTO Customer (id, name, sex, age)
                VALUES (?,?,?,?)"""

        try:
            cursor = conn.cursor()
            cursor.execute(sqlBase)
            row = cursor.fetchone()
            if row == None:
                cursor.execute(sqlBaseNew, (newCustomer.ID, newCustomer.Name, newCustomer.Sex, newCustomer.Age))
                conn.commit() 
            else:
                print("Customer ID already on file, please enter a new ID.")
                quit()
        except Error as e:
            print(e)


class Sales: 
    def __init__ (self, customerId, purchaseDate, purchasedItems, totalAmount):
        self.__customerId = customerId
        self.__purchaseDate = purchaseDate
        self.__purchaseItems = purchasedItems
        self.__totalAmount = totalAmount

    
    @property
    def CustomerID (self):
        return self.__customerId

    @CustomerID.setter
    def CustomerID (self, customerId):
        self.__customerId = customerId   
    
    
    @property
    def PurchaseDate (self):
        return self.__purchaseDate

    @PurchaseDate.setter
    def PurchaseDate (self, purchaseDate):
        self.__purchaseDate = purchaseDate


    @property
    def PurchaseItems (self):
        return self.__purchaseItems

    @PurchaseItems.setter
    def PurchasedItems (self, purchasedItems):
        self.__purchasedItems = purchasedItems


    @property
    def TotalAmount (self):
        return self.__totalAmount

    @TotalAmount.setter
    def TotalAmount (self, totalAmount):
        self.__totalAmount = totalAmount

    def __str__ (self):
        return "({0},{1},{2},{3})".format(self.__customerId, self.__purchaseDate, 
        self.__purchasedItems, self.__totalAmount)

class SalesDataManager:
    @staticmethod
    def CreateNewTable(conn):
        CreateSalesTableSQL =    """
                            CREATE TABLE IF NOT EXISTS sales (
                                id integer,
                                purchase_date text,
                                purchased_items text,
                                total_amount int
                            );
                            """
        try:
            cursor = conn.cursor()
            cursor.execute(CreateSalesTableSQL)
            conn.commit() 
        except Error as e:
            print(e)

    @staticmethod
    def LoadAllData(startDateString, endDateString, conn):
        #Tests to make sure dates were passed in correct form
        try:
            myStartDate = datetime.datetime.strptime(startDateString,"%Y-%m-%d")
        except ValueError as e:
            print(e)
            print("Please Enter proper StartDate in yyyy-mm-dd format")
            quit()
        
        try:
            myEndDate = datetime.datetime.strptime(endDateString,"%Y-%m-%d")
        except ValueError as e:
            print(e)
            print("Please Enter proper EndDate in yyyy-mm-dd-yyyy format")
            quit()

        #Checks that end date does not come before start date.
        if myStartDate > myEndDate:
            print("Please Enter a StateDate that is equal to or before the EndDate")
            quit()

        try:
                cursor = conn.cursor()
                #Clears old values from the table.
                cursor.execute("DELETE FROM Sales")

                fileNameBase = "-SalesData.csv"
                #Creates a list of file names.
                filenameList = []
                for index in range(myStartDate.day, myEndDate.day+1):
                    name = str(myStartDate.year) + "-" + myStartDate.strftime("%m") + "-" + str('%0.2d' % index) + fileNameBase
                    if os.path.isfile(name):
                        filenameList.append(name)
                #Processes each file individually.
                for file in filenameList:
                    sales = pd.read_csv(file)
                    sales.columns = sales.columns.str.replace(r'\s+', '_')
                    sales.Total_Amount = [x.strip('$') for x in sales.Total_Amount]

                    for row in sales.itertuples():
                        entry = (row.CustomerID, row.Purchase_Date, row.Purchased_Items, row.Total_Amount)
    
                        sqlBase =  """INSERT INTO Sales (id, purchase_date, purchased_items, total_amount)
                            VALUES (?,?,?,?)"""

                        cursor.execute(sqlBase, entry)
                    conn.commit() 
        except Error as e:
            print(e)

    @staticmethod
    def AddSale(newSale, conn):

        sqlBaseNew =  """INSERT INTO Sales (id, purchase_date, purchased_items, total_amount)
                VALUES (?,?,?,?)"""

        try:
            cursor = conn.cursor()
            cursor.execute(sqlBaseNew, (newSale.CustomerID, newSale.PurchaseDate, newSale.PurchasedItems, newSale.TotalAmount))
            conn.commit() 
        except Error as e:
            print(e)

def Main():

    #Check that dates are in proper form.
    if len(sys.argv) != 3:
        print("please provide start date and end date in the format yyyy-mm-dd")
        quit()

    startDateString = sys.argv[1]
    endDateString = sys.argv[2]

    #Create Database
    databaseName = "Sales.db"
    conn = Database.GetConnection(databaseName)

    #Create Tables
    CustomerDataManager.CreateNewTable(conn)
    SalesDataManager.CreateNewTable(conn)

    #Load data into SQLite Tables
    CustomerDataManager.LoadAllData("CustomerData.csv", conn)
    SalesDataManager.LoadAllData(startDateString, endDateString, conn)
    
    #Add a new customer
    newCustomer = Customer(100, "Marky Mark", "male", 25)
    CustomerDataManager.AddCustomer(newCustomer, conn)

    #Add a new sale
    newSale = Sales(100, "10/1/2017", "Laptop", 8900)
    SalesDataManager.AddSale(newSale, conn)

    #Pull joined data from SQL
    query = """SELECT Sales.purchase_date, Sales.purchased_items, Sales.total_amount, Customer.name 
                FROM Sales LEFT JOIN Customer ON Sales.id = Customer.id"""
    salesDF = pd.read_sql_query(query, conn)
    
    #Set dollar display formatting.
    pd.options.display.float_format = '${:,.2f}'.format

    #Top 5 customers by sales.
    print("\nTop 5 customers by sales.")
    customerSales = salesDF.groupby(['name']).sum()*1.00
    #Format for $'s.
    print(customerSales.nlargest(5, 'total_amount'))
    
    #Top 3 products by sales.
    print("\nTop 3 products by sales.")
    productSales = salesDF.groupby(['purchased_items']).sum()*1.00
    print(productSales.nlargest(3, 'total_amount'))
    
    #Daily trend in sales per product.
    print("\nDaily sales by product.")
    productSalesDay = salesDF.groupby(['purchase_date', 'purchased_items']).sum()*1.00
    print(productSalesDay)
    #Create a bar graph
    productSalesDay.plot(kind='bar')
    plt.xlabel('Products')
    plt.ylabel('Total Amount')
    plt.title("Product Sales by Day")

    plt.show()

    #Daily trend in sales per customer.
    print("\nDaily sales by customer.")
    customerSalesDay = salesDF.groupby(['purchase_date', 'name']).sum()*1.00
    print(customerSalesDay)
    #Create a bar graph
    customerSalesDay.plot(kind='bar')
    plt.xlabel('Customers')
    plt.ylabel('Total Amount')
    plt.title("Customer Sales by Day")

    plt.show()
    
    #Average quanitity of each product sold per day.
    print("\nAverage quanitity of each product sold.")
    productDayAvg = salesDF.groupby(['purchase_date', "purchased_items"]).count()
    print(productDayAvg['total_amount'])
    #Create a bar graph
    productDayAvg['total_amount'].plot(kind='bar')
    plt.xlabel('Products Day')
    plt.ylabel('Average units sold')
    plt.title("Average Units Sold per Day")

    plt.show()

    #Average sales per day by $.
    print("\nAverage sales per day.")
    salesDayAvg = salesDF.groupby(['purchase_date']).mean()
    print(salesDayAvg)
    #Create a bar graph
    salesDayAvg.plot(kind='bar')
    plt.xlabel('Day')
    plt.ylabel('Average Sales')
    plt.title("Average Sales by Day")

    plt.show()

    #Average sales per day by customer.
    print('\nAverage sales per day by customer')
    customerDayAvg = salesDF.groupby(['purchase_date', 'name']).mean()*1.00
    print(customerDayAvg)
    #Create a bar graph
    customerDayAvg.plot(kind='bar')
    plt.xlabel('Customer/Day')
    plt.ylabel('Average Sales')
    plt.title("Average Sales per Day by Customer")

    plt.show()

    Database.CloseConnection(conn)

if __name__ == "__main__":
    Main()
