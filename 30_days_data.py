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
mycursor.execute( """CREATE TABLE 30day_prices (
  symbol VARCHAR(15) NOT NULL,
  date DATE NOT NULL,
  series VARCHAR(255) NOT NULL,
  open_price FLOAT(10,2) NOT NULL,
  close_price FLOAT(10,2) NOT NULL
  );"""
)

# 5) Prepared a python function to insert data into tables
def insert_into_table(bhavcopy,date):
    sql = "INSERT INTO 30day_prices (symbol,date,open_price, close_price,series) VALUES (%s,%s, %s, %s,%s)"
    val = []
    for row in bhavcopy.itertuples():
        symbol=str(row[1])
        series = str(row[2])
        open_price=float(row[3])
        close_price=float(row[6])
        val.append((symbol,date,open_price,close_price,series))

    mycursor.executemany(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "was inserted.")

# 6) Preparing a python template to fetch the data for last 30 days
i = 1
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


# 7) selecting the data for checking once
mycursor.execute("SELECT * FROM 30day_prices")
for x in mycursor:
    print(x)


# Note: the 30day_prices table contains around 70000 rows 2300 data rows for each day i.e 2300x30.  

# FINAL QUERY 2
mycursor.execute("""select * from 
    (select date, symbol, row_number() over (partition by date order by (close_price-open_price)/open_price desc) as stock_rank           
    from 30day_prices
    where series = "EQ") ranks
    where stock_rank<=25
    """)
for x in mycursor:
    print(*x)

# FINAL QUERY 3
mycursor.execute("""select symbol as sym     
    from 30day_prices
    where series = "EQ"
    group by symbol
    having count(date)=30
    order by ((select close_price from 30day_prices where symbol=sym AND date="2022-12-13" AND series = "EQ"
    group by symbol) - (select open_price from 30day_prices where symbol=sym AND date="2022-11-01" AND series = "EQ"
    group by symbol))/(select open_price from 30day_prices where symbol=sym AND date="2022-11-01" AND series = "EQ"
    group by symbol) DESC limit 25
    """)
for x in mycursor:
    print(*x)

print("Excecution Successfull")