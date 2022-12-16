# This is same code implementing Indexing on prices table to query fast on required dates
# Note: using indexing will result in faster quering but slow future updates on tables.

import pandas as pd
from urllib.error import HTTPError
import mysql.connector
import datetime 

# 1) Connect to the database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="admin@123",
  database="stocks_assignment"
)

# 2) Getting the cursor reference of the connection.
mycursor = mydb.cursor()

# 3) To show the tables of our database
mycursor.execute("SHOW TABLES")
for x in mycursor:
  print(x)

# 4) Created table to store last 30 day data using below command
mycursor.execute( """CREATE TABLE 30day_prices2 (
  symbol VARCHAR(15) NOT NULL,
  date DATE NOT NULL,
  open_price FLOAT(10,2) NOT NULL,
  close_price FLOAT(10,2) NOT NULL
  );"""
)

# 5) Prepared a python function to insert data into tables
def insert_into_table(bhavcopy,date):
    sql = "INSERT INTO 30day_prices2 (symbol,date,open_price,close_price) VALUES (%s,%s,%s,%s)"
    val = []
    for row in bhavcopy.itertuples():
        symbol=str(row[1])
        series = str(row[2])
        open_price=float(row[3])
        close_price=float(row[6])
        if series == "EQ":
            val.append((symbol,date,open_price,close_price))

    # for row in val:
    #     print(row)
    mycursor.executemany(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "was inserted.")

# 6) Preparing a python template to fetch the data for last 30 days
i = 3
todays_date = datetime.datetime.now()
date_count = 0
while(date_count<30):
    date_delta = datetime.timedelta(days = i)
    req_date = todays_date - date_delta

    req_year = str(req_date.year)
    req_month_int = str(req_date.month)
    req_month = str(req_date.strftime("%b").upper())
    # Note we require the day of len 2.
    req_day = str(req_date.day)
    if len(req_day) == 1:
        req_day = "0" + req_day
    
    url = 'https://www1.nseindia.com/content/historical/EQUITIES/' + req_year + '/' + req_month + '/cm' + req_day + req_month + req_year + 'bhav.csv.zip'
    try:
        bhavcopy = pd.read_csv(url)

        required_date =  req_year + "-" + req_month_int + "-" + req_day
        insert_into_table(bhavcopy,required_date)
        date_count+=1
    except HTTPError:
        print("No trading data due to weekend day: ",req_date)
    i+=1


# 7) This is most important step to query fast
mycursor.execute("CREATE INDEX date_index ON 30day_prices2(date)")

answer3 = []
f3 = open('Query_3_Output_op.txt', 'w')
answer3.append("Rank" + "|" + "Name" + "|" + "Symbol" + "|" + "Gains" + "|" + "\n")  

# # FINAL QUERY 3
mycursor.execute("""select stocks.name, 30day_prices2.symbol as sym, ((select A.close_price from 30day_prices2 A where A.symbol=sym AND A.date="2022-12-13") - (select B.open_price from 30day_prices2 B where B.symbol=sym AND B.date="2022-11-01"))/(select C.open_price from 30day_prices2 C where C.symbol=sym AND C.date="2022-11-01") as gains   
    from stocks,30day_prices2
    where stocks.symbol = 30day_prices2.symbol
    group by 30day_prices2.symbol 
    having count(30day_prices2.date)=30
    order by gains
    DESC limit 25
    """)
i = 1
for x in mycursor:
    answer3.append(str(i) + "|" + x[0] + "|" + x[1] + "|" + str(x[2]) + "\n")
    i+=1

f3.writelines(answer3)
f3.close()

print("Excecution Successfull")