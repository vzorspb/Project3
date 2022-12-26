#!/usr/bin/env python3

import re
import clickhouse_connect
from user_agents import parse
import pandas as pd

try:
    client = clickhouse_connect.get_client(host='localhost', username='default', password='')
except:
    print ('Can not connect to Clickhouse server')
    exit()
client.command("CREATE TABLE IF NOT EXISTS httpdlog (ip String, datettime String, method String, path String, resultcode String, size Int32, referer String, useragent String, osfamily String, osversion String, devicefamily String, devicebrand String, devicemodel String, browserfamily String, browserversion String, ismobile Bool, ispc Bool, isbot Bool) ENGINE MergeTree ORDER BY ip")

st=0
counter=0
set=[]
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
                                filter = 7
                                match = re.findall(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(\d\d/.+)\] "(.{3,7}) (/) HTTP/\d.\d" (\d+) (\d+) "(-)" "(.+)"' ,line)

        if len(match)==0:
            errors.write(line)
        else:
            if len(match[0]) != 8:
                print(filter, len(match[0]))
                print(match)            
                print(line)            
            else:
                st=st+1
                counter=counter+1
                newline = match[0]
                ua = parse(newline[7])
                newline = newline + (str(ua.os.family), str(ua.os.version_string), str(ua.device.family), str(ua.device.brand), str(ua.device.model), str(ua.browser.family), str(ua.browser.version_string) , ua.is_mobile or ua.is_tablet,ua.is_pc, ua.is_bot)
                set.append(newline)
                if st>99999:
                    st=0
                    names = ['ip', 'datettime', 'method', 'path', 'resultcode', 'size', 'referer', 'useragent', 'osfamily', 'osversion', 'devicefamily', 'devicebrand', 'devicemodel', 'browserfamily', 'browserversion', 'ismobile', 'ispc', 'isbot']
                    df = pd.DataFrame(set)
                    df.columns = names
                    client.insert_df('httpdlog',df)
                    print(counter)
                    set=[]
df = pd.DataFrame(set)
df.columns = names
client.insert_df('httpdlog',df)

errors.close()


