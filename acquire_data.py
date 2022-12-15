import pandas as pd
import mysql.connector
from dateutil import parser

# imported stocks info from the given URL
stocks_info = pd.read_csv('https://archives.nseindia.com/content/equities/EQUITY_L.csv')
# print(stocks_info)  

# imported bhavcopy info from the given URL
bhavcopy = pd.read_csv('https://archives.nseindia.com/content/historical/EQUITIES/2022/DEC/cm13DEC2022bhav.csv.zip')
# print(bhavcopy)

# Connected to MySQL with the help of mysql connector in python
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="admin@123",
  database="stocks_assignment"
)
# print(my_database)

# Steps to create relational database containing required tables

# 1) Getting the cursor reference of the connection.
mycursor = mydb.cursor()

# 2) Created Database
mycursor.execute("CREATE DATABASE Stocks_Assignment")

# 3) To show all our database
mycursor.execute("SHOW DATABASES")
for x in mycursor:
  print(x)

# 4) Created STOCKS table using below command
mycursor.execute( """CREATE TABLE stocks (  
    symbol VARCHAR(15) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    series VARCHAR(255) NOT NULL,
    date_of_listing VARCHAR(255) NOT NULL,
    paid_up_value INT NOT NULL,
    market_lot INT NOT NULL,
    isin_number VARCHAR(255) NOT NULL,
    face_value INT NOT NULL)"""
)

# 5) show tables
mycursor.execute("SHOW TABLES")
for x in mycursor:
  print(x)


# 6) insert values into the table using for loop
sql = "INSERT INTO stocks (symbol, name, series, date_of_listing,paid_up_value,market_lot,isin_number,face_value) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
val = []
for row in stocks_info.itertuples():
    symbol=str(row[1])
    name=str(row[2])
    series=str(row[3])
    date_of_listing=str(row[4])
    paid_up_value=int(row[5])
    market_lot=int(row[6])
    isin_number=str(row[7])
    face_value=int(row[8])
    val.append((symbol, name, series, date_of_listing,paid_up_value,market_lot,isin_number,face_value))
    
mycursor.executemany(sql, val)
mydb.commit()
print(mycursor.rowcount, "was inserted.")

# 7) selecting the data for checking once
mycursor.execute("SELECT * FROM prices")
for x in mycursor:
    print(x)

# 8) Created Prices table using below command
mycursor.execute( """CREATE TABLE prices (
  symbol VARCHAR(15) NOT NULL,
  series VARCHAR(255) NOT NULL,
  open_price FLOAT(10,2) NOT NULL,
  close_price FLOAT(10,2) NOT NULL )
  );"""
)

# 9) describe table
mycursor.execute("DESCRIBE prices")
for x in mycursor:
    print(x)

# 10) insert values into the table prices using for loop
sql = "INSERT INTO prices (symbol,open_price, close_price,series) VALUES (%s, %s, %s,%s)"
val = []
for row in bhavcopy.itertuples():
    symbol=str(row[1])
    series = str(row[2])
    open_price=float(row[3])
    close_price=float(row[6])
    val.append((symbol,open_price,close_price,series))

mycursor.executemany(sql, val)
mydb.commit()
print(mycursor.rowcount, "was inserted.")

answer = []
f1 = open('Query_1_Output.txt', 'w')
answer.append("Rank" + "|" + "Name" + "|" + "Symbol" + "|" + "Gains" + "|" + "\n")

# FINAL QUERY 1
mycursor.execute("""SELECT stocks.name,prices.symbol, ((prices.close_price-prices.open_price)/prices.open_price) as gains from stocks,prices
    where prices.series = "EQ" AND stocks.symbol = prices.symbol
    order by gains DESC
    limit 25""")
i = 1
for x in mycursor:
    answer.append(str(i) + "|" + x[0] + "|" + x[1] + "|" + str(x[2]) + "|" + "\n")
    i+=1

f1.writelines(answer)
f1.close()

print("Excution Successfull")


