from pyhive import hive
conn = hive.Connection(host="54.164.120.245", port=10000, username="w205")

cursor = conn.cursor()

query = "SHOW TABLES"
print query
cursor.execute(query)
for result in cursor.fetchall():
  print result

query2 = "SELECT * FROM crime_table limit 10"
print query2
cursor.execute(query2)
for result in cursor.fetchall():
  print result

cursor.close()
conn.commit()
conn.close()
