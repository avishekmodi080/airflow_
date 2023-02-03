#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 30 16:39:56 2023

@author: kcpl
"""

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


from datetime import datetime, date
import pandas as pd
import glob
import numpy as np
import os
import mysql.connector
from mysql.connector import Error
import shutil


with DAG(dag_id="see_here", start_date=datetime(2023, 2, 3), schedule_interval="@daily") as dag:
    @task
    def append_csvs():
        source = '/home/kcpl/SPYDER'
        destination = '/home/kcpl/SPYDER/dump'
        os.chdir("/home/kcpl/SPYDER")
        csv_files = glob.glob('*.{}'.format('csv'))
        csv_files
        datetime.now().date()
        counter = 1
        ## appending all the csv files and adding cust_id
        csv_appended = pd.DataFrame().drop_duplicates()
        for file in csv_files:
            df_temp = pd.read_csv(file)
            df_temp['cust_id'] = counter
            counter+= 1
            df_temp['file_name'] = file.split('.')[0]
            df_temp['upload_date'] = datetime.now().date()
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
            shutil.move(src_path, dst_path)
            #return df
    
    ############################db_connection
        try:
            db_connection = mysql.connector.connect(host="localhost", user="root", password="root", database="CredAble")
                        
            curr = db_connection.cursor()
            #curr.execute("DROP TABLE IF EXISTS table_1;")
            curr.execute("""CREATE TABLE IF NOT EXISTS table_1 (
                                txn_date DATE NOT NULL,
                                `description` varchar(355),
                                debit float,
                                credit float,
                                balance double,
                                account_name varchar(355),
                                account_number bigint,
                                mode varchar(30),
                                entity varchar(30),
                                source_of_trans varchar(355),
                                sub_mode varchar(30),
                                transaction_type varchar(30),
                                bank_name varchar(30),
                                lid varchar(355),
                                cust_id int,
                                file_name varchar(355),
                                upload_date DATE NOT NULL
                                );""")
                      
                
            for i,row in df.iterrows():
                sql = """INSERT IGNORE INTO table_1 (txn_date,description,debit,credit,balance,account_name,account_number,`mode`,entity,source_of_trans,sub_mode,transaction_type,bank_name,lid,cust_id, file_name, upload_date)
                                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
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

    append_csvs()
    
        































