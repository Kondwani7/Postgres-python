import psycopg2
from dotenv import load_dotenv
import os
#print(os.getenv('PATH'))
#connect to your db
try:
    con = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv('DB'),
        user = os.getenv('DB_USERNAME'),
        password = os.getenv('DB_PASSWORD'),
        port = os.getenv('DB_PORT')
    )
except psycopg2.Error as e:
    print("Error: failed to connect to DB")
    print(e)
#cursor
try:
    cur = con.cursor()
except psycopg2.Error as e:
    print("Error: failed to connect cursor to the DB")
    print(e)


#execution 
'''
#create table
try:
    cur.execute("CREATE TABLE if not exists music_library(album_id int, \
                                                      album_name varchar, artist_name varchar, \
                                                     year int , songs text[]);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#insert values into rows
try:
    cur.execute("insert into music_library(album_id, album_name, artist_name, year, songs) \
                 values (%s,%s,%s,%s,%s)",\
                     (1,"CLB","Drake", 2010,["N 2 Deep", "Fair trade", "Pipe down","knife talk"]))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into music_library(album_id, album_name, artist_name, year, songs) \
                 values (%s,%s,%s,%s,%s)",\
                     (2,"X","Chris brown", 2014,["X", "Do Better", "Time for love", "Autum leaves"]))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
'''
#make the table into 1NF (normalisation) but making a separate column for song names
'''
#create table
try:
    cur.execute("create table if not exists music_library2(album_id int, \
                                                      album_name varchar, artist_name varchar, \
                                                     year int , song_name varchar);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#insert values into rows
try:
    cur.execute("insert into music_library2(album_id, album_name, artist_name, year, song_name) \
                 values (%s,%s,%s,%s,%s)",\
                     (1,"CLB","Drake", 2010,"N 2 Deep"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into music_library2(album_id, album_name, artist_name, year, song_name) \
                 values (%s,%s,%s,%s,%s)",\
                     (1,"CLB","Drake", 2021,"Fair trade"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into music_library2(album_id, album_name, artist_name, year, song_name) \
                 values (%s,%s,%s,%s,%s)",\
                     (1,"CLB","Drake", 2021,"Pipe down"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into music_library2(album_id, album_name, artist_name, year, song_name) \
                 values (%s,%s,%s,%s,%s)",\
                     (1,"CLB","Drake", 2021,"knife talk"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#chris brown song names
try:
    cur.execute("insert into music_library2(album_id, album_name, artist_name, year, song_name) \
                 values (%s,%s,%s,%s,%s)",\
                     (2,"X","Chris brown", 2014,"Do Better"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into music_library2(album_id, album_name, artist_name, year, song_name) \
                 values (%s,%s,%s,%s,%s)",\
                     (2,"X","Chris brown", 2014,"X"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into music_library2(album_id, album_name, artist_name, year, song_name) \
                 values (%s,%s,%s,%s,%s)",\
                     (2,"X","Chris brown", 2014,"Time for love"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into music_library2(album_id, album_name, artist_name, year, song_name) \
                 values (%s,%s,%s,%s,%s)",\
                     (2,"X","Chris brown", 2014,"Autum leaves"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
'''
#2ND form by creating 2 separate tables for a song and album library
'''
#create album library table
try:
    cur.execute("create table if not exists album_library(album_id int, \
                                                      album_name varchar, artist_name varchar, \
                                                     year int);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)

#create song library table
try:
    cur.execute("create table if not exists song_library( song_id int, album_id int, \
                                                      song_name varchar);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#inserting  values into the album library
try:
    cur.execute("insert into album_library(album_id, album_name, artist_name, year) \
                 values (%s,%s,%s,%s)",\
                     (1,"CLB","Drake", 2010))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into album_library(album_id, album_name, artist_name, year) \
                 values (%s,%s,%s,%s)",\
                     (2,"X","Chris brown", 2014))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#insert values into song library
try:
    cur.execute("insert into song_library(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (1, 1, "N 2 Deep"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (2, 1, "Fair Trade"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (3, 1, "Pipe down"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (4, 1, "knife talk"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#chris brown songs
try:
    cur.execute("insert into song_library(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (1, 2, "X"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (2, 2, "Do Better"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (3, 2, "Time for love"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (4, 2, "Autum leaves"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
'''
#join the two tables
'''
try:
    cur.execute("SELECT * FROM album_library JOIN\
                song_library ON album_library.album_id = song_library.album_id")
except psycopg2.Error as e:
    print("Error joining tables")
    print(e)
'''
#3NF check for transitive dependencies, creating a artist_library table
#create album library table
'''
try:
    cur.execute("create table if not exists album_library2(album_id int, \
                                                      album_name varchar, artist_id int, artist_name varchar, \
                                                     year int);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)

#create song library table
try:
    cur.execute("create table if not exists song_library2( song_id int, album_id int, \
                                                      song_name varchar);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#create artist library table
try:
    cur.execute("CREATE TABLE IF NOT EXISTS artist_library(artist_id int, artist_name varchar);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#inserting  values into the album library
try:
    cur.execute("insert into album_library2(album_id, album_name,artist_id, artist_name, year) \
                 values (%s,%s,%s,%s,%s)",\
                     (1,"CLB",1,"Drake", 2010))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into album_library2(album_id, album_name, artist_id, artist_name, year) \
                 values (%s,%s,%s,%s,%s)",\
                     (2,"X",2,"Chris brown", 2014))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#insert values into song library
try:
    cur.execute("insert into song_library2(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (1, 1, "N 2 Deep"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library2(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (2, 1, "Fair Trade"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library2(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (3, 1, "Pipe down"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library2(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (4, 1, "knife talk"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#chris brown songs
try:
    cur.execute("insert into song_library2(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (1, 2, "X"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library2(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (2, 2, "Do Better"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library2(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (3, 2, "Time for love"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library2(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (4, 2, "Autum leaves"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#inserting values into the artist library
try:
    cur.execute("INSERT INTO artist_library(artist_id, artist_name)\
                 VALUES(%s,%s)", \
                        (1,"Drake"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("INSERT INTO artist_library(artist_id, artist_name)\
                 VALUES(%s,%s)", \
                        (2,"Chris brown"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#join tables
try:
    cur.execute("SELECT * FROM(artist_library JOIN album_library2 ON \
                 artist_library2.artist_id = album_library2.artist_id) JOIN\
                 song_library2 ON album_library2.album_id = song_library2.album_id;")
except psycopg2.Error as e:
    print("Error joining tables")
    print(e)
'''



#DENORMALIZATION: faster read time, slower right time

'''
#create similar tables like before, plus and additional song length table
try:
    cur.execute("create table if not exists album_library3(album_id int, \
                                                      album_name varchar, artist_id int, artist_name varchar, \
                                                     year int);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)

#create song library table
try:
    cur.execute("create table if not exists song_library3( song_id int, album_id int, \
                                                      song_name varchar);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#create artist library table
try:
    cur.execute("CREATE TABLE IF NOT EXISTS artist_library2(artist_id int, artist_name varchar);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#create song length table
try:
    cur.execute("CREATE TABLE IF NOT EXISTS song_length(song_id int, song_length int);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#inserting  values into the album library
try:
    cur.execute("insert into album_library3(album_id, album_name,artist_id, artist_name, year) \
                 values (%s,%s,%s,%s,%s)",\
                     (1,"CLB",1,"Drake", 2010))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into album_library3(album_id, album_name, artist_id, artist_name, year) \
                 values (%s,%s,%s,%s,%s)",\
                     (2,"X",2,"Chris brown", 2014))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#insert values into song library
try:
    cur.execute("insert into song_library3(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (1, 1, "N 2 Deep"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library3(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (2, 1, "Fair Trade"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library3(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (3, 1, "Pipe down"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library3(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (4, 1, "knife talk"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#chris brown songs
try:
    cur.execute("insert into song_library3(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (5, 2, "X"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library3(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (6, 2, "Do Better"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library3(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (7, 2, "Time for love"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library3(song_id, album_id, song_name) \
                 values(%s,%s,%s)", \
                     (8, 2, "Autum leaves"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#inserting values into the artist library
try:
    cur.execute("INSERT INTO artist_library2(artist_id, artist_name)\
                 VALUES(%s,%s)", \
                        (1,"Drake"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("INSERT INTO artist_library2(artist_id, artist_name)\
                 VALUES(%s,%s)", \
                        (2,"Chris brown"))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#insert into song length
try:
    cur.execute("INSERT INTO song_length(song_id, song_length) \
                 VALUES(%s,%s)", \
                     (1,276))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e) 
try:
    cur.execute("INSERT INTO song_length(song_id, song_length) \
                 VALUES(%s,%s)", \
                     (2,292))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e) 
try:
    cur.execute("INSERT INTO song_length(song_id, song_length) \
                 VALUES(%s,%s)", \
                     (3,292))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e) 
try:
    cur.execute("INSERT INTO song_length(song_id, song_length) \
                 VALUES(%s,%s)", \
                     (4,207))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("INSERT INTO song_length(song_id, song_length) \
                 VALUES(%s,%s)", \
                     (5,243))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("INSERT INTO song_length(song_id, song_length) \
                 VALUES(%s,%s)", \
                     (6,229))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("INSERT INTO song_length(song_id, song_length) \
                 VALUES(%s,%s)", \
                     (7,233))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("INSERT INTO song_length(song_id, song_length) \
                 VALUES(%s,%s)", \
                     (8,279))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
'''
#Denormalize the tables by combining the songs_library3 and song_length tables and the album_library3 & artist_library3 tables
'''
try:
    cur.execute("create table if not exists album_library4(album_id int, \
                                                      album_name varchar, artist_id int, artist_name varchar, \
                                                     year int);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#create song library table
try:
    cur.execute("create table if not exists song_library4( song_id int, album_id int, \
                                                      song_name varchar, song_length int);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#inserting values into the album_library4
try:
    cur.execute("insert into album_library4(album_id, album_name,artist_id, artist_name, year) \
                 values (%s,%s,%s,%s,%s)",\
                     (1,"CLB",1,"Drake", 2010))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
try:
    cur.execute("insert into album_library4(album_id, album_name, artist_id, artist_name, year) \
                 values (%s,%s,%s,%s,%s)",\
                     (2,"X",2,"Chris brown", 2014))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

# inserting into the song_library4 
try:
    cur.execute("insert into song_library4(song_id, album_id, song_name, song_length) \
                 values(%s,%s,%s, %s)", \
                     (1, 1, "N 2 Deep",276))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library4(song_id, album_id, song_name, song_length) \
                 values(%s,%s,%s, %s)", \
                     (2, 1, "Fair Trade",292))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library4(song_id, album_id, song_name,song_length) \
                 values(%s,%s,%s, %s)", \
                     (3, 1, "Pipe down",292))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library4(song_id, album_id, song_name,song_length) \
                 values(%s,%s,%s,%s)", \
                     (4, 1, "knife talk",207))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
#chris brown songs
try:
    cur.execute("insert into song_library4(song_id, album_id, song_name,song_length) \
                 values(%s,%s,%s,%s)", \
                     (5, 2, "X",243))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library4(song_id, album_id, song_name,song_length) \
                 values(%s,%s,%s,%s)", \
                     (6, 2, "Do Better",229))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library4(song_id, album_id, song_name,song_length) \
                 values(%s,%s,%s,%s)", \
                     (7, 2, "Time for love",233))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)

try:
    cur.execute("insert into song_library4(song_id, album_id, song_name,song_length) \
                 values(%s,%s,%s,%s)", \
                     (8, 2, "Autum leaves",279))
except psycopg2.Error as e:
    print("Error inserting values")
    print(e)
'''
#creating facts and dimensions tables
#fact table
'''
try:
    cur.execute("create table if not exists customer_transactions(customer_id int, \
                                                      store_id int, spent numeric);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)
#insertion into fact table
try:
    cur.execute("insert into customer_transactions(customer_id, store_id, spent) \
                 values(%s,%s,%s)", \
                     (1, 1, 30.45))
except psycopg2.Error as e:
    print("error creating customer transactions fact table", e)
try:
    cur.execute("insert into customer_transactions(customer_id, store_id, spent) \
                 values(%s,%s,%s)", \
                     (2, 1, 100.00))
except psycopg2.Error as e:
    print("error creating customer transactions fact table", e)
'''
#items purchased dimesions table
'''
try:
    cur.execute("create table if not exists items_purchased(customer_id int, \
                                                      item_number int, item_name varchar);")
except psycopg2.Error as e:
    print("Error creating items_purchased table",e)
#insertions
try:
    cur.execute("insert into items_purchased(customer_id, item_number, item_name) \
                 values(%s,%s,%s)", \
                     (1,1,"Airpods") )
except psycopg2.Error as e:
    print("Error inserting into store table", e)

try:
    cur.execute("insert into items_purchased(customer_id, item_number, item_name) \
                 values(%s,%s,%s)", \
                     (2,3,"Infinix") )
except psycopg2.Error as e:
    print("Error inserting into store table", e)
#store dimesions table
try:
    cur.execute("create table if not exists store(store_id int, city varchar);")
except psycopg2.Error as e:
    print("Error creating store table",e)
#insertions
try:
    cur.execute("insert into store(store_id, city)\
                 values(%s, %s)",\
                       (1,"Lusaka"))
except psycopg2.Error as e:
    print("error inserting into store table")
try:
    cur.execute("insert into store(store_id, city)\
                 values(%s, %s)",\
                       (2,"Kitwe"))
except psycopg2.Error as e:
    print("error inserting into store table")
#creating the customer table
try:
    cur.execute("create table if not exists customer(customer_id int, name varchar, rewards boolean);")
except psycopg2.Error as e:
    print("error creating customer table ", e)
#insertions
try:
    cur.execute("insert into customer(customer_id, name, rewards) \
                values(%s, %s, %s)", \
                      (1, "Kondwani", False))
except psycopg2.Error as e:
    print("error inserting into customer table", e)
try:
    cur.execute("insert into customer(customer_id, name, rewards) \
                values(%s, %s, %s)", \
                      (1, "Muwemi", True))
except psycopg2.Error as e:
    print("error inserting into customer table", e)
'''
#Q1, find customers who spent more than 50 at a store, their name, what they bought and if they're are a rewards member
'''
try:
    cur.execute("select name, item_name, rewards from ((customer_transactions \
                                                 join customer on customer.customer_id=customer_transactions.customer_id) \
                                                 join items_purchased on \
                                                 customer_transactions.customer_id=items_purchased.customer_id)\
                                                 where spent> 50;")
except psycopg2.Error as e:
    print("Error performing Q1 query", e)
'''
#find out how much each store sold
try:
    cur.execute("select store_id, sum(spent) from customer_transactions group by store_id;")
except psycopg2.Error as e:
    print("error answering q2", e)

#execute cursor
cur.execute('select * from customer')
#get each row
rows = cur.fetchall()
for r in rows:
    print(f"customer_id {r[0]}, name {r[1]}, rewards {r[2]}")

#commit changes
con.commit()
#close cursor
cur.close()

#close the db connection
con.close()