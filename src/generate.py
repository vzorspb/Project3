#!/usr/bin/env python3

import clickhouse_connect
import pandas as pd

def print_table (uniq_users,actions, table_name, is_find, list_in_col):
    result = client.query('select count(ip) FROM (SELECT ip,useragent as users FROM default.httpdlog WHERE '+is_find+'=True GROUP BY ip,useragent)')
    users=result.result_set[0][0]
    result = client.query('select count(ip) FROM default.httpdlog WHERE '+is_find+'=True')
    i_actions=result.result_set[0][0]
    print('-------------------------------------------------------------------------------------------------------------')
    print('     '+table_name.ljust(20)+'			:  ',users,"(","{:.4f}".format(users/uniq_users*100),"%)		 ", i_actions,'(',"{:.4f}".format(i_actions/actions*100),"%)")
    print('-------------------------------------------------------------------------------------------------------------')
    result = client.query('select '+list_in_col+', count('+list_in_col+') as cc FROM (SELECT '+list_in_col+' FROM default.httpdlog WHERE '+is_find+'=True GROUP BY ip,useragent,'+list_in_col+') GROUP BY '+list_in_col+' ORDER BY cc DESC ')
    list = result.result_set
    for x in list:
        st=0
        result = client.query("select count(browserfamily) FROM default.httpdlog WHERE "+is_find+"=True and "+list_in_col+"='"+x[0]+"'")
        browsers=result.result_set[0][0]

        result = client.query("select count(ip) FROM default.httpdlog WHERE "+is_find+"=True and "+list_in_col+"='"+x[0]+"'")
        item_actions=result.result_set[0][0]
        result = client.query("select browserfamily, count(browserfamily) as cc FROM default.httpdlog WHERE "+is_find+"=True and "+list_in_col+"='"+x[0]+"' GROUP BY browserfamily ORDER BY cc DESC limit 5")
#        print("select browserfamily, count(browserfamily) as cc FROM default.httpdlog WHERE "+is_find+"=True and "+list_in_col+"='"+x[0]+"' GROUP BY browserfamily ORDER BY cc DESC limit 5")
#        exit()
       
        browser_list=result.result_set
        for browser in browser_list:
            st=st+1
            result = client.query("select resultcode, count(resultcode) as cc FROM default.httpdlog WHERE not resultcode='200' and "+is_find+"=True and "+list_in_col+"='"+x[0]+"' GROUP BY resultcode ORDER BY resultcode")
            resultcode = result.result_set
            if st==1:
                print('                    ',x[0].ljust(20),'	:', str(x[1]).rjust(8),'(',"{:.4f}".format(x[1]/uniq_users*100),'%)  ','	',str(item_actions).rjust(8), '(',"{:.4f}".format(item_actions/actions*100),'%)	',browser[0],'/',browser[1],'/',int(browser[1]/browsers*100),'%',resultcode, sep='')
            else:
                print('												',browser[0],'/',browser[1],'/',int(browser[1]/browsers*100),'%',resultcode, sep='')


try:
    client = clickhouse_connect.get_client(host='localhost', username='default', password='')
except:
    print ('Can not connect to Clickhouse server')
    exit()

# Количество уникальных пользователей
result = client.query('select count(ip) FROM (SELECT ip,useragent as users FROM default.httpdlog GROUP BY ip,useragent)')
uniq_users=result.result_set[0][0]
# Количество действий пользователей
result = client.query('select count(ip) FROM default.httpdlog')
actions=result.result_set[0][0]
# Количество действий пользователей



print('Количество уникальных посетителей		:',uniq_users,'		Количество действий:',actions)
print("   из них:")

# Количество мобильных пользователей
print_table(uniq_users,actions,'мобильные устройства', 'ismobile', 'osfamily')
# Количество стационарных пользователей
print_table(uniq_users,actions,'стационарные устройства', 'ispc', 'osfamily')
# Количество роботов
print_table(uniq_users,actions,'роботы', 'isbot', 'browserfamily')
