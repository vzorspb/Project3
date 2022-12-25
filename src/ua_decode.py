from user_agents import parse
import psycopg2

try:
    connection = psycopg2.connect(user='vzor',host='10.8.0.1',port='5432',database='logdb')
except:
    print('Connection to database error.')
    exit()

cursor = connection.cursor()
#sql='ALTER TABLE public.useragent ADD COLUMN device varchar; ALTER TABLE public.useragent ADD COLUMN os varchar; ALTER TABLE public.useragent ADD COLUMN browser varchar;'
#cursor.execute(sql)
#connection.commit()
sql="select id, ua_string from public.useragent;"
cursor.execute(sql)
rows=cursor.fetchall()
st=0
for row in rows:
    st=st+1
    print(row[0],row[1])
    ua=parse(row[1])
#    print('    device:',ua.device.family)
#    print('        os:',ua.os.family, ua.os.version_string)
#    print('   browser:',ua.browser.family,ua.browser.version_string)
    device = ua.device.family
    os = ua.os.family+' '+ua.os.version_string
    browser=ua.browser.family+' '+ua.browser.version_string
    sql="UPDATE public.useragent SET device='"+device+"', os='"+os+"', browser='"+browser+"'WHERE id='"+str(row[0])+"';"
    cursor.execute(sql)
    if st>1000:
        st=0
        connection.commit()
connection.commit()
connection.close()

