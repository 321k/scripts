from bs4 import BeautifulSoup
import mysql.connector
import requests
import re
import datetime


# Connect to Looker DB from local
connection = mysql.connector.connect(user='stats', password='Kalamaja123',host='127.0.0.1',database='obfuscated',port=3307)
connection

cursor = connection.cursor()

# Get list of latest business websites
fetch_data_query =  """SELECT user_profile.user_id, webpage 
	FROM user_profile 
	LEFT JOIN tmp_website_description w ON w.user_id = user_profile.user_id 
	WHERE (LENGTH(webpage) > 1) 
	AND (date_created >= '2016-01-01') 
	AND w.user_id IS NULL 
	ORDER BY 1 DESC
	LIMIT 30;"""


cursor.execute(fetch_data_query)
rows = cursor.fetchall()
rows


# Correct URL formatting of websites
def format_url(url):
    url = url.lower()
    if not url.startswith('http'):
        return 'http://' + url
    return url.lower()

formattedRows = [[row[0], format_url(row[1])] for row in rows]


# Get website descriptions
description = []
for i in range(0,len(formattedRows)):
	row = formattedRows[i][1]
	description.append('NA')
	try:
		r = requests.get(row)
	except requests.exceptions.RequestException as e:
		print('No description found')
		continue
	r = requests.get(row)
	soup = BeautifulSoup(r.content, 'html.parser')
	html = soup.findAll('meta[name=description]')
	if len(html) != 0:
		description[i] = html[0].string
		print(description[i])
		continue
	html = soup.findAll('p', {'class': 'hero textalign-c'})
	if len(html) != 0:
		description[i] = html[0].string
		print(description[i])
		continue
	html = soup.findAll('title')
	if len(html) != 0:
		description[i] = html[0].string
		print(description[i])
		continue
	print('No description found')



# Insert data into db
columns = ['user_id', 'website_description', 'date_added']
for i in range(0, len(rows)):
	user_id = rows[i][0]
	website_description = description[i]
	date_added = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
	values = [user_id, website_description, date_added]
	values_string = "('"+"','".join(map(str, values))+"')"
	columns_string = ",".join(map(str, columns))
	sql = "insert into obfuscated.tmp_website_description (%s) values %s;" % (columns_string, values_string)
	cursor.execute(sql)
	connection.commit()

# Close db connection
connection.close()
cursor.close()
