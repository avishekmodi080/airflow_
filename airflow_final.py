#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  8 11:24:31 2023

@author: kcpl
"""


from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.email_operator import EmailOperator
import pendulum

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.message import EmailMessage
import smtplib

from datetime import time, date, timedelta, datetime
from csv import writer
import datetime as dt
import time
import pandas as pd
import glob
import numpy as np
import os
import mysql.connector
from mysql.connector import Error
import shutil
#from goto import goto

def mail1():
    msg=EmailMessage()
    msg['Subject']='Data push failed'
    msg['From']='airflowdag123@gmail.com'
    msg['To']='airflowdag123@gmail.com'
    msg.set_content("Alert!!! There is no csv file to append.")
    server=smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login('airflowdag123@gmail.com','cnwbudpsdgnunevb')
    server.send_message(msg)
    print('Mail Sent')


def append_csvs():
        source = '/home/kcpl/SPYDER'
        destination = '/home/kcpl/SPYDER/dump'
        os.chdir("/home/kcpl/SPYDER")
        csv_files = glob.glob('*.{}'.format('csv'))
        csv_files
        path = '/home/kcpl/SPYDER/dump'
        #dump_files = glob.glob('/home/kcpl/SPYDER/dump/*.{}'.format('csv'))
        dump_files = glob.glob(path+'/*.csv')
        dump_files = [i.split('/')[5] for i in dump_files]
        csv_files = [i for i in csv_files if i not in dump_files]
# =============================================================================
#         for i in csv_files[:]:
#             if i in dump_files:
#                 csv_files.remove(i)
#                 dump_files.remove(i)
#      
#         print("list1 : ", a)
#         print("list2 : ", b)
#      
# =============================================================================
# =============================================================================
#         csv_file_1 = pd.DataFrame(csv_files)
#         csv_file_1.to_csv('record.csv', index=False)
# # ===========================================================================
# #         with open ('record.csv','a') as f_object:
# #             writer_object = writer(f_object)
# #             writer_object.writerow(csv_file_1)
# #             f_object.close()
# # ===========================================================================
#         df_1 = pd.read_csv('record.csv')
#         df_1 = pd.concat([csv_file_1], keys = ['0'], ignore_index=True)
#         #record = df['file_name'].unique()
#         #record = pd.Series(record)
#         #record.to_csv('record.csv', index=False)
# =============================================================================
        if len(csv_files)>=1:   
            datetime.now().date()
            with open ('sequence_count.txt', 'rt') as myfile: 
                contents = myfile.read() 
                a = int(contents)             
            a
            #counter = 1
            ## appending all the csv files and adding cust_id
            csv_appended = pd.DataFrame().drop_duplicates()
            for file in csv_files:
                df_temp = pd.read_csv(file)
                #df_temp['cust_id'] = counter
                df_temp['cust_id'] = a
                a+= 1
                #counter+= 1
                t = time.localtime()
                current_time = time.strftime("%H:%M:%S", t)
                df_temp['file_name'] = file.split('.')[0]
                df_temp['upload_date'] = datetime.now().date()
                df_temp['upload_time'] = current_time
                csv_appended = csv_appended.append(df_temp, ignore_index=True)
                src_path = os.path.join(source, file)
                dst_path = os.path.join(destination, file)
                #cvs_appended = df_temp.to_csv('df_temp_copy.csv')        
                #csv_appended.to_csv()
                    
                ## copying the original csv file to df
                df = csv_appended.copy()
                        
                ## replacing nan values with 0:
                df = df.replace(np.nan, 0)
                        
                ##cleaning the df column names: 
                df.columns = df.columns.str.lower().str.replace(" ", "_")
                df.reset_index(drop=True, inplace=True)
                        
                        #df.head() 
                ## changing the date format of df to sql format- %y-%m-%d:
                df['txn_date'] = pd.to_datetime(df['txn_date'])
                df['txn_date'] = pd.to_datetime(df['txn_date'],format="%y-%m-%d") ## removing quotes in account_number column:
                df['account_number'] = df['account_number'].apply(lambda x: x.replace("'",''))
                df['upload_time'] = pd.to_datetime(df['upload_time'], format='%H:%M:%S').dt.time
                #df.info()
                shutil.move(src_path, dst_path)
                #return df
                    
            b = df['cust_id'].max()
            with open('sequence_count.txt', 'w') as f:
                f.write('{}'.format(b+1))
                f.close()
                   
            ############################db_connection
            try:
                db_connection = mysql.connector.connect(host="localhost", user="root", password="root", database="CredAble")
                                
                curr = db_connection.cursor()
                #curr.execute("DROP TABLE IF EXISTS table_1;")
                curr.execute("""CREATE TABLE IF NOT EXISTS table_1 (
                                        txn_date DATE NOT NULL,
                                        `description` varchar(355) NOT NULL,
                                        debit float NOT NULL,
                                        credit float NOT NULL,
                                        balance double NOT NULL,
                                        account_name varchar(355) NOT NULL,
                                        account_number bigint NOT NULL,
                                        `mode` varchar(30) NOT NULL,
                                        entity varchar(30) NOT NULL,
                                        source_of_trans varchar(355) NOT NULL,
                                        sub_mode varchar(30) NOT NULL,
                                        transaction_type varchar(30) NOT NULL,
                                        bank_name varchar(30) NOT NULL,
                                        lid varchar(355) NOT NULL,
                                        cust_id int NOT NULL,
                                        file_name varchar(355) NOT NULL,
                                        upload_date DATE NOT NULL,
                                        upload_time TIME NOT NULL
                                        );""")
                              
                        
                for i,row in df.iterrows():
                    sql = """INSERT IGNORE INTO table_1 (txn_date,description,debit,credit,balance,account_name,account_number,`mode`,entity,source_of_trans,sub_mode,transaction_type,bank_name,lid,cust_id, file_name, upload_date, upload_time)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                    curr.execute(sql, tuple(row))
                db_connection.commit()
                print(curr.rowcount, "Record inserted successfully into table_1")
                curr.close()
            except mysql.connector.Error as error:
                print('Failed to insert records in table_1 {}'.format(error))
                    
            finally:
                if db_connection.is_connected():
                    db_connection.close()
                    print("MySQL connection is closed")
                    
        else:
            mail1()

def wait_for_upload():
        time.sleep(60)
        
def check_again():
            csv_files = glob.glob('*.{}'.format('csv'))
            if len(csv_files)>=1:
                print('csv folders are there')
                append_csvs()
            else:
                print('no files in the directory')
                
def sleep_after_append():
            time.sleep(60)
  
    
def handling_left_over_csvs():
    os.chdir("/home/kcpl/SPYDER")
    source = '/home/kcpl/SPYDER'
    destination = '/home/kcpl/SPYDER/dump'
    csv_files_1 = glob.glob('*.{}'.format('csv'))
    for file in csv_files_1:
        src_path = os.path.join(source, file)
        dst_path = os.path.join(destination, file)
        if len(csv_files_1)>=1:
            shutil.move(src_path, dst_path)
        else:
            print('You are done!')
        
                
               
                
def_args = {
    'owner' : 'airflow',
    #'start_date' : airflow.utils.dates.days_ago(0),
    'depends_on_past' : True, ## if set True the current task won't run if task in previous fails 
    'email_on_failure' : True,
    'email' : ['airflowdag123@gmail.com'],
    'email_on_retry' : True,
    # If a task fails, retry it once after waiting
    'retries' : 1,
    'retry_delay' : timedelta(seconds=30),
    }


with DAG("avishek_2",start_date=datetime(2023,2,7),schedule_interval= timedelta(minutes=5),catchup=False,max_active_runs=1, default_args=def_args) as dag:
    
    
    append_csv= PythonOperator(
    task_id='append_csv',
    email_on_failure=True,
    python_callable = append_csvs
    )
    
    wait_time= PythonOperator(
    task_id='wait_time',
    email_on_failure=True,
    python_callable = wait_for_upload
    )
    
    
    re_check = PythonOperator(
    task_id='re_check',
    email_on_failure=True,
    python_callable = check_again
    )
    
    sleep_time = PythonOperator(
    task_id='sleep_time',
    email_on_failure=True,
    python_callable = sleep_after_append
    )
    
    moving_extra_csvs = PythonOperator(
    task_id='moving_extra_csvs',
    email_on_failure=True,
    python_callable = handling_left_over_csvs
    )

append_csv>>wait_time>>re_check>>sleep_time>>moving_extra_csvs
