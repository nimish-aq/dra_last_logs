import mysql.connector
from mysql.connector import Error as SQL_ERR

from datetime import timedelta
from datetime import datetime, timedelta
import dateutil.relativedelta
import traceback
import time
import csv
from os import listdir
from os.path import isfile, join
import sys
from dateutil import parser
current_time = datetime.utcnow() - timedelta(hours=0)
lower_time = datetime.utcnow() - timedelta(days = 1)

print(lower_time)
def mysql_connection():
        try:
                mysql_conn = mysql.connector.connect(host="10.221.86.35", user="root", database="gm_operations_reporting",password="Ttpl@123", port= "3307")
        except Exception:
                print ("error")
        return mysql_conn

db = mysql_connection()
cursor = db.cursor()


q = "Select distinct Successful_bs_IMSI as BS_IMSI,(if(Successful_Attempts,Successful_Attempts,0) + if(Total_failure_Attempts,Total_failure_Attempts,0)) as Attempts FROM vw_esim_success_fail_partial where (Successful_Local_IMSI is not null or  Failure_Local_imsi is not null) and Success_Profile_Date >= '%s' and Success_Profile_Date <= '%s' order by Attempts desc;"%(lower_time, current_time)
cursor.execute(q)
data = cursor.fetchall()

print(data)
lst =[]
for BS_IMSI in data:
    #print(BS_IMSI)

    q_1 = "select Username, ResultCode, Time_Stamp from dra where dra.Username = '%s' and dra.CommandCode = 'Update-Location-Request' order by ID ASC limit 1;"%BS_IMSI[0]
    cursor.execute(q_1)

    data_list = cursor.fetchall()
    print(data_list)
    print(data_list[0][2])
    q_2 = "insert into dra_initial_test(Username, ResultCode , Time_Stamp) values (%s,%s,%s);"
    val = (data_list[0][0],data_list[0][1],data_list[0][2])
    print(q_2)
    cursor.execute(q_2, val)
    db.commit()

    #lst.append(data_list)
cursor.close()



