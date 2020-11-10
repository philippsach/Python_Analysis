import pymysql
import pandas as pd

# tutorial 1: connect
con = pymysql.connect(user="phil_sach",
                      password="entthesis2020",
                      host="85.214.204.221",
                      database="thesis")

cursor = con.cursor()

sql_query = "SELECT VERSION()"

try:
    cursor.execute(sql_query)
    data = cursor.fetchone()
    print("Database Version: %s" % data)

except Exception as e:
    print("Exception : ", e)

# show all tables in database
cursor.execute("USE thesis")
cursor.execute("SHOW TABLES")

# two options: either return data from last query or iterate over the cursor
tables = cursor.fetchall()
print(tables)
for(table_name,) in cursor:
    print(table_name)

# show information about how many records we have for each category
try:
    cursor.execute("show table status")
    # indices = [0, 2]
    # information = [cursor.fetchall()[index] for index in indices]
    information = cursor.fetchall()
    print(information)
    table_names = []
    table_rows = []
    for tup in information:
        table_names.append(tup[0])
        table_rows.append(tup[4])
    print(table_names)
    print(table_rows)
    #tables_info = pd.DataFrame(information, columns = )
except Exception as e:
    print("Exception: ", e)

# create a pandas dataframe with names and number of rows
kickstarter_share_per_type = pd.DataFrame(list(zip(table_names, table_rows)),
                                          columns = ["Name", "Entries"])

print(kickstarter_share_per_type)

kickstarter_share_per_type = kickstarter_share_per_type[kickstarter_share_per_type["Name"] != "links"]
#print(kickstarter_share_per_type)
#print(kickstarter_share_per_type["Entries"].sum())
kickstarter_share_per_type["Share"] = kickstarter_share_per_type.Entries / kickstarter_share_per_type["Entries"].sum()
#print(kickstarter_share_per_type)
print(kickstarter_share_per_type["Share"].sum()) # should equal 1, otherwise there is a mistake

kickstarter_share_per_type = kickstarter_share_per_type.sort_values(by = ["Share"], ascending = False)
print(kickstarter_share_per_type)


# try out a select statement
try:
    cursor.execute("SELECT * FROM art")
    rows = cursor.fetchall()
except Exception as e:
    print("Exception: ", e)

# tutorial 2: create table in mysql database

# delete_existing_table = "drop table if exists employee"
# create_table_query = """create table employee(
# _id int auto_increment primary key,
# name varchar(20) not null
# )"""

# try :
#    cursor.execute( delete_existing_table )
#    print("Existing table has been deleted")
#    cursor.execute( create_table_query )
#    print("Employee table has been created")

# except Exception as e:
#    print("Exception occured:", e)

print("This is the end")

con.close()
