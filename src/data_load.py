#!/usr/bin/env python2.6

import psycopg2
import re

connection = psycopg2.connect(user="vzor",
                                  host="10.8.0.1",
                                  port="5432",
                                  database="logdb")
                                  
cursor = connection.cursor()
st=0
skeep=0
errors=open('../log/access.err', 'w')
with open('../log/access.log', 'r') as file:
    for line in file:
        filter = 1
	match = re.findall(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(\d\d/.+)\] "(.{3,7}) (/.+) HTTP/\d.\d" (\d+) (\d+) "(\S+)" "(.+)" "-"' ,line)
	if len(match)==0:
	    filter = 2
	    match = re.findall(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(\d\d/.+)\] "(.{3,7}) (/\S+) HTTP/\d.\d" (\d+) (\d+) "(-)" "(.+)" "-"' ,line)
            if len(match)==0:
                filter = 3
	        match = re.findall(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(\d\d/.+)\] "(.{3,4}) (/\S+) HTTP/\d.\d" (\d+) (\d+) "(-)" "(.+)"' ,line)
	        if len(match)==0:
	            filter = 4
	            match = re.findall(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(\d\d/.+)\] "(.{3,7}) (/) HTTP/\d.\d" (\d+) (\d+) "(.+)" "(-)"' ,line)
	            if len(match)==0:
	                filter = 5
	                match = re.findall(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(\d\d/.+)\] "(.{3,7}) (.+) HTTP/\d.\d" (\d+) (\d+) "(-)" "(-)" "-"' ,line)
	                if len(match)==0:
                            filter = 6
	                    match = re.findall(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(\d\d/.+)\] "(.{3,7}) (/.+) HTTP/\d.\d" (\d+) (\d+) "(\S+)" "(.+)" "\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}"' ,line)

	if len(match)==0:
            errors.write(line)
#            print(line)
        else:
            skeep=skeep+1  
            if len(match[0]) != 8:
                print(filter, len(match[0]))
                print(match)            
                print(line)            
            else:
                st=st+1
                sql="INSERT INTO logdb.public.httpdlog(ip, datetime, method, path, resultcode, size, referer, useragent) VALUES ('"+"', '".join(str(x).replace("'","").replace("\\x","") for x in match[0])+"');"

		if skeep>00000:
#		    print(sql)
                    cursor.execute(sql)
                if st>5000:
                    connection.commit()
                    st=0
                    print(skeep)
connection.commit()

exit()


st=0
with open('../log/client_hostname.csv', 'r') as file:
    for line in file:
        line_list=line.split(',')
        if line_list[0] != 'client':
            st=st+1
            if line_list[2]=='[Errno 1] Unknown host':
                line_list=[line_list[0],'Unknown host']
            else:
                line_list=[line_list[0],line_list[1]]
            sql="INSERT INTO logdb.public.client_hostname(ip, hostname) VALUES ('"+line_list[0]+"', '"+line_list[1]+"');"
#            print(sql)
            cursor.execute(sql)
            if st>10000:
               connection.commit()
               st=0
               print(".")
connection.commit()
errors.close()


