#!/usr/bin/env python
# coding: utf-8

# In[133]:
import plotly.graph_objects as go
import streamlit as st
import pandas as pd
from pandas.tseries.offsets import *
from sqlalchemy import create_engine
import mysql.connector
import hashlib

import urllib.request
external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')


buggy = mysql.connector.connect(**st.secrets["mysql"])
st.set_page_config(layout="wide")

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def run(DATE):
    print(external_ip)
    df_pickups_old_cars = pd.read_sql("""
    SELECT 
        ca.id 'agreement_id',
        ca.start_date,
        MAX(ca2.id) 'last_agreement_id',
        MAX(ca2.end_date) end_date,
        MAX(ca.car_id) car_id
    FROM
        cars_agreement ca
            LEFT JOIN
        cars_agreement ca2 ON ca.car_id = ca2.car_id
    WHERE
        ca.id > ca2.id
            AND cast(ca.start_date as DATE) = "%s"
            AND ca.stage >= 10
            AND ca2.stage >= 10
    GROUP BY ca.id
    """%DATE,con=buggy)

    df_pickups = pd.read_sql("""
    SELECT 
        ca.id 'agreement_id',
        ca.start_date 'start_date',
        ca.car_id,
        CONCAT(cd.first_name, ' ', cd.last_name) 'Driver Name',
        cd.tlc_license "TLC License",
        ca.is_contract_signed,
        au.username "Rep"
    FROM
        cars_agreement ca
            INNER JOIN
        cars_driver cd ON cd.id = ca.driver_id
            Left join auth_user au on au.id = ca.representative_id
    WHERE
        CAST(ca.start_date AS DATE) ="%s"
            AND ca.stage >= 10
    """%DATE,con=buggy)


    df_pickups_new_cars = df_pickups[~df_pickups.agreement_id.isin(df_pickups_old_cars.agreement_id)]
    df_pickups_today = pd.concat([df_pickups_old_cars,df_pickups_new_cars[["agreement_id","start_date","car_id"]]])
    sets = (df_pickups_today.car_id.astype("str") +",\""+ df_pickups_today.end_date.astype("str") +"\",\""+  df_pickups_today.start_date.astype("str")+"\" , " +df_pickups_today.agreement_id.astype("str")).to_list()

    zero = sets[0]
    temp_table = "SELECT "+zero[0] +" AS car_id ," +zero[1]+" AS end_date, "+zero[2]+" AS start_date, " +zero[3]+" AS agreement_id UNION SELECT "  + " UNION SELECT ".join(sets[1:])


    df_forms = pd.read_sql(temp_table,con=buggy)
    df_forms = pd.read_sql("""
    SELECT 
       map.filled_date , tab.car_id,tab.agreement_id
    FROM
        `mobile_app_formhistory` map
        inner join (%s) tab on tab.car_id = map.car_id
        
    WHERE
        form_type = 'CHECKOUT' and (map.filled_date > tab.end_date and CAST(map.filled_date as DATE) <= CAST(tab.start_date as DATE) ) ;
    """%temp_table,con=buggy)

    df_forms = df_forms.groupby(["agreement_id"])["car_id"].count().reset_index().rename({"car_id":"checkout"},axis=1)

    df_pickups_today  =  df_pickups.merge(df_forms,on="agreement_id",how="left")


    df_pickups_today['Pickup Date'] = pd.to_datetime(df_pickups_today['start_date']).dt.strftime('%Y-%B-%d')
    df_pickups_today['Pickup Time'] = pd.to_datetime(df_pickups_today['start_date']).dt.strftime('%H:%M:%S')
    df_pickups_today["Agreement ID"] =  df_pickups_today["agreement_id"]



    df_pickups_today = df_pickups_today[["Agreement ID" , "Pickup Date" , "Pickup Time" , "Driver Name" , "TLC License" ,"Rep","checkout","is_contract_signed"]]


    df_pickups_today ["Contract"]= ' '
    df_pickups_today ["Checkout"]= ' '
    df_pickups_today.loc[df_pickups_today.is_contract_signed > 0 , "Contract"] = '\u2705'
    df_pickups_today.loc[df_pickups_today.checkout > 0 , "Checkout"] = '\u2705'
    df_pickups_today = df_pickups_today.drop(["checkout","is_contract_signed"],axis=1)

    df_pickups_today.index += 1 

    df_pickups_today = df_pickups_today.style.applymap(lambda x: "background-color: red" if x==' ' else "background-color: white",subset=["Contract" , "Checkout"])

    st.table(df_pickups_today)


DATE = "2021-09-17"
user_input = st.text_input("Add Password")

if st.button("Check"):
    if len(user_input)>0 :  
        if make_hashes(user_input) == make_hashes(st.secrets["mysql"]["password"]):
            run(DATE)
        else:
            "Wrong Password"


