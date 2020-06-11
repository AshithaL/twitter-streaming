import MySQLdb

connection = MySQLdb.connect(host='localhost',
                             db='twitter_analysis',
                             user='root',
                             passwd='nineleaps')

conn = connection.cursor()
