import streamlit as st
from streamlit_option_menu import option_menu
import os
import json 
import pandas as pd
import plotly.express as px
import requests

#D:\youtubescrap\phonepe\pulse\phonepe.py

import mysql.connector
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="phonepeproject"
)
print(mydb)
mycursor = mydb.cursor(buffered=True)



#AGGREGATED TRANSACTION:

mycursor.execute(f"SELECT Years ,States, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM aggre_transaction  GROUP BY States, Years")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
AT1 = pd.DataFrame(rows, columns=['Years' ,'States', 'Transaction_count', 'Transaction_amount'])


def AgreeTransaction_amt_count_y(year):
    mycursor.execute(f"SELECT States, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM aggre_transaction WHERE Years = {year} GROUP BY States")
    rows = mycursor.fetchall()
    
    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States', 'Transaction_count', 'Transaction_amount'])
    #print(df)
    fig_amount = px.bar(df, x="States", y="Transaction_amount", title=f"{year} TRANSACTION AMOUNT")
    st.plotly_chart (fig_amount)

    fig_count = px.bar(df, x="States", y="Transaction_count", title=f"{year} TRANSACTION COUNT")
    st.plotly_chart (fig_count)

    fig_choropleth_amount = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_amount',
        color_continuous_scale='temps',
        title=f"{year} TRANSACTION AMOUNT"
    )
    fig_choropleth_amount.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth_amount)

    fig_choropleth_count = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States", 
        color='Transaction_count',
        color_continuous_scale="viridis",
        title=f"{year} TRANSACTION COUNT"
    )
    fig_choropleth_count.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth_count)
    return df



mycursor.execute(f"SELECT Quarter ,States, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM aggre_transaction GROUP BY States,Quarter")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
AT2  = pd.DataFrame(rows, columns=['Quarter', 'States', 'Transaction_count','Transaction_amount'])


def AgreeTransaction_amt_count_Q(Quarter):
    mycursor.execute(f"SELECT States, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM aggre_transaction WHERE Quarter = {Quarter} GROUP BY States")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States', 'Transaction_count','Transaction_amount'])


    fig_amount = px.bar(df, x="States", y="Transaction_amount", title=f"{Quarter} TRANSACTION AMOUNT", color_discrete_sequence=px.colors.sequential.dense_r)
    st.plotly_chart(fig_amount)

 
    fig_count = px.bar(df, x="States", y="Transaction_count", title=f"{Quarter} TRANSACTION COUNT", color_discrete_sequence=px.colors.sequential.algae_r)
    st.plotly_chart(fig_count)

   
    fig_choropleth_amount = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_amount',
        color_continuous_scale='temps',
        title=f"{Quarter} TRANSACTION AMOUNT"
    )
                    
    fig_choropleth_amount.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth_amount)
    

    fig_choropleth_count = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States", 
        color='Transaction_count',
        color_continuous_scale="viridis",
        title=f"{Quarter} TRANSACTION COUNT"
    )
    fig_choropleth_count.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth_count)
    

 



mycursor.execute(f"SELECT States ,Transaction_type, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM aggre_transaction GROUP BY Transaction_type, States")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
AT3 = pd.DataFrame(rows, columns=['States','Transaction_type', 'Transaction_count', 'Transaction_amount'])


def AgreeTransaction_amt_count_transtype(States):
    mycursor.execute(f"SELECT States ,Transaction_type, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM aggre_transaction GROUP BY Transaction_type, States")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['State','Transaction_type', 'Transaction_count', 'Transaction_amount'])

    fig_pie_1 = px.pie(data_frame=df, names="Transaction_type", values="Transaction_amount", width=600, 
                       title=f"{States.upper()} TRANSACTION AMOUNT", hole=0.5)
    st.plotly_chart(fig_pie_1)

    fig_pie_2 = px.pie(data_frame=df, names="Transaction_type", values="Transaction_count", width=600, 
                       title=f"{States.upper()} TRANSATION COUNT", hole=0.5)
    st.plotly_chart(fig_pie_2)



#AGGREGATED USER:

mycursor.execute(f"SELECT Years ,Brands, SUM(Transaction_count) AS Transaction_count\
                      FROM aggre_user  GROUP BY  Brands,Years")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
AU1  = pd.DataFrame(rows, columns=['Years','Brands', 'Transaction_count'])

def Agreeuser_year(Years):
    mycursor.execute(f"SELECT Brands, SUM(Transaction_count) AS Transaction_count\
                      FROM aggre_user WHERE Years = {Years} GROUP BY  Brands")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['Brands', 'Transaction_count'])


    fig1  = px.bar(df , x="Brands", y="Transaction_count", title= f"{Years}TRANSACTION COUNT VS BRAND ", width =1000
                       ,color_discrete_sequence= px.colors.sequential.haline, hover_name = "Brands")
    st.plotly_chart(fig1)
    


mycursor.execute(f"SELECT Brands,Quarter , SUM(Transaction_count) AS Transaction_count\
                                   FROM aggre_user GROUP BY Brands ,Quarter")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
AU2 = pd.DataFrame(rows, columns=['Brands', 'Quarter','Transaction_count'])

       
def Agreeuser_yearq(quarter):
       mycursor.execute(f"SELECT Brands, SUM(Transaction_count) AS Transaction_count\
                            FROM aggre_user WHERE Quarter = {quarter} GROUP BY Brands")
       rows = mycursor.fetchall()

       columns = [col[0] for col in mycursor.description]
       df = pd.DataFrame(rows, columns=['Brands', 'Transaction_count'])


       fig2  = px.bar(df, x="Brands", y="Transaction_count", title= f" {quarter} QUARTER TRANSACTION COUNT VS BRAND ", width =1000
                            ,color_discrete_sequence= px.colors.sequential.Magenta_r, hover_name = "Brands")
       
       st.plotly_chart(fig2)
     

mycursor.execute(f"SELECT States ,Brands, SUM(Transaction_count) AS Transaction_count,avg(Percentage) AS Percentage\
                      FROM aggre_user GROUP BY Brands ,States")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
AU3 = pd.DataFrame(rows, columns=['States', 'Brands', 'Transaction_count', 'Percentage'])


def Agreeuser_states(state):
    mycursor.execute(f"SELECT Brands, SUM(Transaction_count) AS Transaction_count,avg(Percentage) AS Percentage\
                      FROM aggre_user WHERE States = '{state}' GROUP BY Brands")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['Brands', 'Transaction_count', 'Percentage'])


    fig3 = px.line(df , x="Brands", y="Transaction_count", hover_data= "Percentage" ,title= "BRANDS ,TRANSACTIONCOUNT , PERCENTAGE"
                  , width =1000,color_discrete_sequence= px.colors.sequential.Peach, markers= True)
    st.plotly_chart(fig3)


 # MAP TRANS:

mycursor.execute(f"SELECT cast(Years as int )Years , States, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM map_trans  GROUP BY States, Years")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
MT1  = pd.DataFrame(rows, columns=['Years','States', 'Transaction_count', 'Transaction_amount'])


def mapTransaction_amt_count_y(year):
    mycursor.execute(f"SELECT States, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM map_trans WHERE Years = {year} GROUP BY States")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States', 'Transaction_count', 'Transaction_amount'])

  
    fig_amount = px.bar(df, x="States", y="Transaction_amount", title=f"{year} TRANSACTION AMOUNT")
    st.plotly_chart(fig_amount)

    

    fig_count = px.bar(df, x="States", y="Transaction_count", title=f"{year} TRANSACTION COUNT")
    st.plotly_chart(fig_count)

    fig_choropleth = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_amount',
        color_continuous_scale='turbo',
        title=f"{year} TRANSACTION AMOUNT "
    )
    fig_choropleth.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth)
   

    fig_choropleth = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_count',
        color_continuous_scale='temps',
        title=f"{year} TRANSACTION COUNT "
    )
    fig_choropleth.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth)
    



mycursor.execute(f"SELECT States,Quarter, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM map_trans GROUP BY States,Quarter")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
MT2 = pd.DataFrame(rows, columns=['States','Quarter' ,'Transaction_count', 'Transaction_amount'])


def mapTransaction_amt_count_q(Quarter):
    mycursor.execute(f"SELECT States, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM map_trans WHERE Quarter = {Quarter} GROUP BY States")
    rows = mycursor.fetchall()
    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States', 'Transaction_count', 'Transaction_amount'])

    AMOUNT = px.bar(df, x="States", y="Transaction_amount", title = f"{Quarter} QUARTER TRANSACTION AMOUNT")
    st.plotly_chart(AMOUNT)
    COUNT = px.line(df, x="States", y="Transaction_count", title =f"{Quarter}QUARTER  TRANSACTION COUNT")
    st.plotly_chart(COUNT)
  
    fig_choropleth = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_amount',
        color_continuous_scale='turbo',
        title="TRANSACTION AMOUNT "
    )
    fig_choropleth.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth)

    fig_choropleth = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_count',
        color_continuous_scale='ylorrd',
        title="TRANSACTION COUNT "
    )
    fig_choropleth.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth)


mycursor.execute(f"SELECT States ,Districts, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM map_trans  GROUP BY Districts ,States")
rows = mycursor.fetchall()
columns = [col[0] for col in mycursor.description]
MT3 = pd.DataFrame(rows, columns=['States','Districts', 'Transaction_count', 'Transaction_amount'])
def mapTransaction_amt_count_dis(States):
    mycursor.execute(f"SELECT Districts, SUM(Transaction_count) AS Transaction_count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM map_trans WHERE States = '{States}' GROUP BY Districts")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['Districts', 'Transaction_count', 'Transaction_amount'])
   
    fig_pie_2 = px.bar(df,  x = "Transaction_amount", y = "Districts" , orientation= "h" ,
                       title = f"{States.upper()} DISTRICTS AND TRANSACTION AMOUNT",color_discrete_sequence= px.colors.sequential.haline)
    st.plotly_chart(fig_pie_2)
    fig_pie_3 = px.bar(df,  x = "Transaction_count", y = "Districts" , orientation= "h" ,
                       title = f"{States.upper()} DISTRICTS AND TRANSACTION COUNT",color_discrete_sequence= px.colors.sequential.Sunset)
    st.plotly_chart(fig_pie_3)


    


# # #MAP USER:

mycursor.execute(f"SELECT Years , States, SUM(RegisteredUsers) AS RegisteredUsers, SUM(AppOpens) AS AppOpens \
                      FROM map_user GROUP BY  Years ,States")
rows = mycursor.fetchall()
columns = [col[0] for col in mycursor.description]
MU1   = pd.DataFrame(rows, columns=['Years','States', 'RegisteredUsers', 'AppOpens'])
def Mapuser_year(years):
    mycursor.execute(f"SELECT States, SUM(RegisteredUsers) AS RegisteredUsers, SUM(AppOpens) AS AppOpens \
                      FROM map_user  WHERE Years = {years} GROUP BY States")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States', 'RegisteredUsers', 'AppOpens'])

    fig1  = px.line(df , x="States", y=["RegisteredUsers","AppOpens"], title= "REGISTEREDUSER VS APPONENS", width =1000 ,height= 900 ,color_discrete_sequence= px.colors.sequential.haline, markers=True)
    st.plotly_chart(fig1)




mycursor.execute(f"SELECT Quarter ,States, SUM(RegisteredUsers) AS RegisteredUsers, SUM(AppOpens) AS AppOpens \
                      FROM map_user  GROUP BY States, Quarter")
rows = mycursor.fetchall()
columns = [col[0] for col in mycursor.description]
MU2 = pd.DataFrame(rows, columns=['Quarter','States', 'RegisteredUsers', 'AppOpens'])
def Mapuser_yQ(Quarter):
    mycursor.execute(f"SELECT States, SUM(RegisteredUsers) AS RegisteredUsers, SUM(AppOpens) AS AppOpens \
                      FROM map_user  WHERE Quarter = {Quarter} GROUP BY States")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States', 'RegisteredUsers', 'AppOpens'])
    
    fig2  = px.line(df , x="States", y=["RegisteredUsers","AppOpens"], title=f"{Quarter} REGISTEREDUSER VS APPONENS", width =1000 ,height= 900 ,color_discrete_sequence= px.colors.sequential.OrRd_r, markers=True)
    st.plotly_chart(fig2)







mycursor.execute(f"SELECT States ,Districts, SUM(RegisteredUsers) AS RegisteredUsers, SUM(AppOpens) AS AppOpens \
                      FROM map_user  GROUP BY Districts , States")
rows = mycursor.fetchall()
columns = [col[0] for col in mycursor.description]
MU3 = pd.DataFrame(rows, columns=['States','Districts', 'RegisteredUsers', 'AppOpens'])

def Mapuser_yqs(State):
    mycursor.execute(f"SELECT Districts, SUM(RegisteredUsers) AS RegisteredUsers, SUM(AppOpens) AS AppOpens \
                      FROM map_user WHERE States = '{State}' GROUP BY Districts")
    rows = mycursor.fetchall()
    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['Districts', 'RegisteredUsers', 'AppOpens'])
    fig3 = px.bar(df , x="RegisteredUsers", y="Districts", title= "State VS REGISTERUSER ", orientation= "h" ,
                   height= 800,color_discrete_sequence= px.colors.sequential.Rainbow)
    st.plotly_chart(fig3)
    fig4 = px.bar(df , x="AppOpens", y="Districts", title= "State VS  Appopens", orientation= "h" ,
                   height=800,color_discrete_sequence= px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig4)


 #TOP TRANS;


mycursor.execute(f"SELECT Years,States, SUM(Transaction_Count) AS Transaction_Count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM top_trans  GROUP BY States ,Years")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
TT1= pd.DataFrame(rows, columns=['Years','States', 'Transaction_Count', 'Transaction_amount'])

def TopTransaction_amt_count_y(year):
    mycursor.execute(f"SELECT States, SUM(Transaction_Count) AS Transaction_Count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM top_trans WHERE Years = {year} GROUP BY States")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States', 'Transaction_Count', 'Transaction_amount'])

    fig_amount = px.bar(df, x="States", y="Transaction_amount", title=f"{year} TRANSACTION AMOUNT",
                        color_discrete_sequence= px.colors.sequential.RdPu_r)
    st.plotly_chart(fig_amount)


    fig_count = px.bar(df, x="States", y="Transaction_Count", title=f"{year} TRANSACTION COUNT",
                       color_discrete_sequence= px.colors.sequential.Plasma)
    st.plotly_chart(fig_count)

    # Plot Choropleth Map
    fig_choropleth = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_amount',
        color_continuous_scale='thermal',
        title=f"{year} TRANSACTION AMOUNT "
    )
    fig_choropleth.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth)

    fig_choropleth = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_Count',
        color_continuous_scale='ylgn',
        title=f"{year} TRANSACTION COUNT "
    )
    fig_choropleth.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth)



mycursor.execute(f"SELECT Quarter, States, SUM(Transaction_Count) AS Transaction_Count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM top_trans  GROUP BY States ,Quarter")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
TT2  = pd.DataFrame(rows, columns=['Quarter','States', 'Transaction_Count', 'Transaction_amount'])




def TopTransaction_amt_count_q(Quarter):

    mycursor.execute(f"SELECT States, SUM(Transaction_Count) AS Transaction_Count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM top_trans WHERE Quarter = {Quarter} GROUP BY States")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States', 'Transaction_Count', 'Transaction_amount'])

            
    AMOUNT = px.bar(df, x="States", y="Transaction_amount", title =f"{Quarter} QUARTER TRANSACTION AMOUNT",
                    height= 800 ,color_discrete_sequence= px.colors.sequential.PuBuGn_r)
    st.plotly_chart(AMOUNT)

    COUNT = px.line(df, x="States", y="Transaction_Count", title =f" {Quarter} QUARTER  TRANSACTION COUNT",
                    height= 800 ,color_discrete_sequence= px.colors.sequential.YlGnBu_r)
    st.plotly_chart(COUNT)

    # Plot Choropleth Map
    fig_choropleth = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_amount',
        color_continuous_scale='ylgnbu_r',
        title= "TRANSACTION AMOUNT "
    )
    fig_choropleth.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth)

    fig_choropleth = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations="States",
        color='Transaction_Count',
        color_continuous_scale='viridis_r',
        title="TRANSACTION COUNT "
    )
    fig_choropleth.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_choropleth)



mycursor.execute(f"SELECT States ,Quarter,Pincodes, SUM(Transaction_Count) AS Transaction_Count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM top_trans group by States ,Quarter,Pincodes")
rows = mycursor.fetchall()
columns = [col[0] for col in mycursor.description]
TT3 = pd.DataFrame(rows, columns=['States','Quarter','Pincodes','Transaction_Count', 'Transaction_amount'])


def topTransaction_amt_count_s(States):

    mycursor.execute(f"SELECT States ,Quarter,Pincodes, SUM(Transaction_Count) AS Transaction_Count, SUM(Transaction_amount) AS Transaction_amount \
                      FROM top_trans WHERE States = '{States}' group by States ,Quarter,Pincodes")
    rows = mycursor.fetchall()
    columns = [col[0] for col in mycursor.description]
    dfs = pd.DataFrame(rows, columns=['States','Quarter','Pincodes','Transaction_Count', 'Transaction_amount'])
    #print(dfs)
  
    fig_pie_1 = px.line(dfs,  y = "Quarter", x = "Transaction_amount" , hover_data = "Pincodes",
                       title = f"{States.upper()}  AND TRANSACTION AMOUNT",color_discrete_sequence= px.colors.sequential.Redor_r)
    st.plotly_chart(fig_pie_1)


    fig_pie_2 = px.line(dfs,  x = "Transaction_Count", y = "Quarter" , hover_data = "Pincodes",
                       title = f"{States.upper()}  AND TRANSACTION COUNT",color_discrete_sequence= px.colors.sequential.ice)
    st.plotly_chart(fig_pie_2)
  


#TOP USER:
mycursor.execute(f"SELECT Years ,States, SUM(RegisteredUsers) AS RegisteredUsers, Quarter\
                      FROM top_user GROUP BY States,Quarter,Years")
rows = mycursor.fetchall()

columns = [col[0] for col in mycursor.description]
TU1 = pd.DataFrame(rows, columns=['Years' ,'States','RegisteredUsers','Quarter']) 

def Topuser_year(year):
    mycursor.execute(f"SELECT States, SUM(RegisteredUsers) AS RegisteredUsers, Quarter\
                      FROM top_user WHERE Years = {year} GROUP BY States,Quarter")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States','RegisteredUsers','Quarter'])

    fig1  = px.bar(df , x="States", y="RegisteredUsers",color="Quarter", title= f"{year}REGISTEREDUSER", width =1000 
                    ,height= 900 ,color_discrete_sequence= px.colors.sequential.BuGn, hover_data="States")
    st.plotly_chart(fig1)


mycursor.execute(f"SELECT States, SUM(RegisteredUsers) AS RegisteredUsers, Quarter\
                      FROM top_user GROUP BY States,Quarter")
rows = mycursor.fetchall()
columns = [col[0] for col in mycursor.description]
TU2 = pd.DataFrame(rows, columns=['States','RegisteredUsers','Quarter'])


def Topuser_yqs(state):

    mycursor.execute(f"SELECT States, SUM(RegisteredUsers) AS RegisteredUsers, Quarter\
                      FROM top_user WHERE States  = '{state}' GROUP BY States,Quarter")
    rows = mycursor.fetchall()

    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(rows, columns=['States','RegisteredUsers','Quarter'])

    fig3 = px.bar(df , x="Quarter", y="RegisteredUsers", title= "REGISTERUSER ", width=1000,color="RegisteredUsers",
                   height= 800,  color_continuous_scale= px.colors.sequential.Rainbow)
    st.plotly_chart(fig3)





  


#STREAMLIT PART:

st.set_page_config(layout="wide")
st.title("PHONEPE DATA VISUALIZATION AND EXPLORATION")


with st.sidebar:

       select = option_menu("Main Menu",["HOME", "DATA VISULAIZATION","INSHGHTS"])


if select == "HOME":
       st.title("STEP1:   CLONE THE DATA FROM GITUB") 
       st.title("STEP2:    CONVERT INTO DATAFRAME AND PUT IN SQL")  
       st.title("STEP3:     DIFFERENT TYPE OF VISULAIZATION") 

elif select == "DATA VISULAIZATION":
       
       tab1, tab2,tab3  =  st.tabs(["AGGREGATED  ANALYSIS" , "MAP ANALYSIS" ,"TOP ANALYSIS"])

       with tab1:
              method = st.radio("SELECT THE METHOD",["AGGREGATED TRANSACTION ANALYSIS","AGGREGATED USER ANALYSIS"])

              if method == "AGGREGATED TRANSACTION ANALYSIS":
                    
                    year = st.slider("SELECT THE YEARS",AT1["Years"].min(), AT1["Years"].max(),AT1["Years"].min())
                    AgreeTransaction_amt_count_y(year)


                    Quarter = st.slider("SELECT THE Quarter",AT2["Quarter"].min(), AT2["Quarter"].max(),AT2["Quarter"].min())
                    AgreeTransaction_amt_count_Q(Quarter)
     

                    States = st.selectbox("SELECT THE STATE",AT3["States"].unique())
                    AgreeTransaction_amt_count_transtype('States')


                   
              elif method == "AGGREGATED USER ANALYSIS":
                    
                    AgreeYears = st.slider("SELECT THE YEARS",AU1["Years"].min(), AU1["Years"].max(),AU1["Years"].min())
                    Agreeuser_year(AgreeYears)

                    AgreeQuarter = st.slider("SELECT THE Quarter",AU2["Quarter"].min(), AU2["Quarter"].max(),AU2["Quarter"].min())
                    Agreeuser_yearq(AgreeQuarter)


                    AgreeStates = st.selectbox("SELECT THE STATE",AU3["States"].unique())
                    Agreeuser_states(AgreeStates)

       with tab2:
              method2 = st.radio("SELECT THE METHOD",["MAP TRANSACTION ANALYSIS"," MAP USER ANALYSIS"])     
              if method2 == "MAP TRANSACTION ANALYSIS":
                    #pass

                     Mapyear = st.slider("SELECT THE YEARS", MT1["Years"].min(), MT1["Years"].max(), MT1["Years"].min(), key='trans_years_slider')
                     mapTransaction_amt_count_y(Mapyear)


                     MapQuarter = st.slider("SELECT THE Quarter", MT2["Quarter"].min(), MT2["Quarter"].max(), MT2["Quarter"].min(),key='trans_Quarter_slider')
                     mapTransaction_amt_count_q(MapQuarter)

                            
                     MapStates = st.selectbox("SELECT THE STATE", MT3["States"].unique())
                     mapTransaction_amt_count_dis(MapStates)


                     
              elif method2 == " MAP USER ANALYSIS":
                    
                     mapuserYears = st.slider("SELECT THE YEARS",MU1["Years"].min(),MU1["Years"].max(),MU1["Years"].min(),key='mapuser_years_slider')
                     Mapuser_year(mapuserYears)

                     mu = st.slider("SELECT THE Quarter",MU2["Quarter"].min(), MU2["Quarter"].max(),MU2["Quarter"].min(),key='mapuser_Quarter_slider')
                     Mapuser_yQ(mu)

                     mapuserStates = st.selectbox("SELECT THE STATE",MU3["States"].unique())
                     Mapuser_yqs(mapuserStates)
                     

       with tab3:
              method3 = st.radio("SELECT THE METHOD",["TOP TRANSACTION ANALYSIS"," TOP USER ANALYSIS"])     
              if method3 == "TOP TRANSACTION ANALYSIS":
                    
                    A = st.slider("SELECT THE YEARS",TT1["Years"].min(),TT1["Years"].max(),TT1["Years"].min(),key='top_years_slider')
                    TopTransaction_amt_count_y(A)



                    B = st.slider("SELECT THE Quarter",TT2["Quarter"].min(), TT2["Quarter"].max(),TT2["Quarter"].min(),key='top_Quarter_slider')
                    TopTransaction_amt_count_q(B)



                    C = st.selectbox("SELECT THE STATE",TT3["States"].unique(),key='state_selectbox')
                    topTransaction_amt_count_s(C)


              elif method3 == " TOP USER ANALYSIS":

                A1 = st.slider("SELECT THE YEARS",TU1["Years"].min(),TU1["Years"].max(),TT1["Years"].min(),key='topuser_years_slider')
                Topuser_year(A1)
                                    
                                    
                B1 = st.selectbox("SELECT THE STATE",TU2["States"].unique(),key='topstate_selectbox')

                Topuser_yqs(B1)

elif select == "INSHGHTS":
    question  =  st.selectbox("SELECT THE QUESTION",["1. Transaction Amount of Aggregated Transaction in asceding order",
                              "2. Transaction Amount of Aggregated Transaction in desc order",
                              "3. AVG Transaction Amount   of Aggregated Transaction ",
                              "4. Transaction count of Aggregated Transaction in asceding order",
                              "5. Transaction Amount of Map Transaction",
                              "6. Transaction Amount  and count of Top Transaction",
                              "7. Transaction Count  of Aggregated  User",
                              "8. Registered user of Map User",
                              "9. AppOpens of Map User",
                              "10. Registered users of Top User"])
        

    if question ==  "1. Transaction Amount of Aggregated Transaction in asceding order":
        query1  = '''SELECT States,SUM(Transaction_amount) AS Transaction_amount 
                        FROM aggre_transaction group BY States ORDER BY Transaction_amount LIMIT 10;'''
        mycursor.execute(query1)
        table=mycursor.fetchall()
        mydb.commit()
        df_1 = pd.DataFrame(table, columns=['States',  'Transaction_amount'])
        fig_amount = px.bar(df_1, x="States", y="Transaction_amount", title="TRANSACTION AMOUNT",color_discrete_sequence= px.colors.sequential.Aggrnyl,
                            hover_name="States")
        st.plotly_chart(fig_amount)




    elif question == "2. Transaction Amount of Aggregated Transaction in desc order":
        query2  = '''SELECT States,SUM(Transaction_amount) AS Transaction_amount 
                        FROM aggre_transaction group BY States ORDER BY Transaction_amount  DESC LIMIT 10;'''
        mycursor.execute(query2)
        table=mycursor.fetchall()
        mydb.commit()
        df_2 = pd.DataFrame(table, columns=['States',  'Transaction_amount'])
        fig_amount = px.bar(df_2, x="States", y="Transaction_amount", title="TRANSACTION AMOUNT",
                                color_discrete_sequence= px.colors.sequential.Pinkyl,hover_name="States")
        st.plotly_chart(fig_amount)




    elif question  == "3. AVG Transaction Amount   of Aggregated Transaction ":
        query3 = '''SELECT States,AVG(Transaction_amount) AS Transaction_amount 
                        FROM aggre_transaction group BY States ORDER BY Transaction_amount;'''
        mycursor.execute(query3)
        table=mycursor.fetchall()
        mydb.commit()
        df_3 = pd.DataFrame(table, columns=['States',  'Transaction_amount'])
        fig_amount = px.bar(df_3, y="States", x="Transaction_amount", title="TRANSACTION AMOUNT",orientation="h",
                                color_discrete_sequence= px.colors.sequential.PuBu, hover_name="States")
        st.plotly_chart(fig_amount)     



    elif question == "4. Transaction count of Aggregated Transaction in asceding order":
        query4  = '''SELECT States,SUM(Transaction_count) AS Transaction_count
                        FROM aggre_transaction group BY States ORDER BY Transaction_count LIMIT 10;'''
        mycursor.execute(query4)
        table=mycursor.fetchall()
        mydb.commit()
        df_4 = pd.DataFrame(table, columns=['States',  'Transaction_count'])
        fig_count = px.line(df_4, x="States", y="Transaction_count", title="TRANSACTION COUNT",color_discrete_sequence= px.colors.sequential.Purpor,
                        hover_name="States")
        st.plotly_chart(fig_count)


    elif question  == "5. Transaction Amount of Map Transaction": 
        query5  = '''SELECT States,SUM(Transaction_amount) AS Transaction_amount 
                        FROM map_trans group BY States ORDER BY Transaction_amount LIMIT 10;'''
        mycursor.execute(query5)
        table=mycursor.fetchall()
        mydb.commit()
        df_5 = pd.DataFrame(table, columns=['States',  'Transaction_amount'])
        fig_amount = px.bar(df_5, x="States", y="Transaction_amount", title="TRANSACTION AMOUNT",
                            color_discrete_sequence= px.colors.sequential.Magma,
                            hover_name="States")
        st.plotly_chart(fig_amount)   



    elif question ==   "6. Transaction Amount  and count of Top Transaction":
        query6  = '''SELECT States,SUM(Transaction_amount) AS Transaction_amount 
                        FROM top_trans group BY States ORDER BY Transaction_amount LIMIT 10;'''
        mycursor.execute(query6)
        table=mycursor.fetchall()
        mydb.commit()
        df_6 = pd.DataFrame(table, columns=['States',  'Transaction_amount'])
        fig_amount = px.line(df_6, x="States", y="Transaction_amount", title="TRANSACTION AMOUNT",
                            color_discrete_sequence= px.colors.sequential.Magma,
                            hover_name="States")
        st.plotly_chart(fig_amount)


    elif question == "7. Transaction Count  of Aggregated  User":
        query7  = '''SELECT States,SUM(Transaction_count) AS Transaction_count
                        FROM aggre_user group BY States ORDER BY Transaction_count LIMIT 10;'''
        mycursor.execute(query7)
        table=mycursor.fetchall()
        mydb.commit()
        df_7 = pd.DataFrame(table, columns=['States',  'Transaction_count'])
        fig_count = px.line(df_7, x="States", y="Transaction_count", title="TRANSACTION COUNT",color_discrete_sequence= px.colors.sequential.Rainbow_r,
                        hover_name="States")
        st.plotly_chart(fig_count)
        

    elif question == "8. Registered user of Map User":
        query8  = '''SELECT districts,SUM(Registeredusers) AS Registeredusers
                        FROM map_user group BY districts ORDER BY RegisteredUsers LIMIT 10;'''
        mycursor.execute(query8)
        table=mycursor.fetchall()
        mydb.commit()
        df_8 = pd.DataFrame(table, columns=['districts',  'RegisteredUsers'])
        fig_amount = px.bar(df_8, x="districts", y="RegisteredUsers", title="TOP 10 RESISTERED USER",color_discrete_sequence= px.colors.sequential.Purpor,
                        hover_name="districts")
        st.plotly_chart(fig_amount)


    elif question ==   "9. AppOpens of Map User":
        query9 = '''SELECT districts,SUM(AppOpens) AS AppOpens
                        FROM map_user group BY districts ORDER BY AppOpens DESC LIMIT 10;'''
        mycursor.execute(query9)
        table=mycursor.fetchall()
        mydb.commit()

        df_9 = pd.DataFrame(table, columns=['districts',  'AppOpens'])
        fig_amount = px.line(df_9, x="districts", y="AppOpens", title="AppOpens",color_discrete_sequence= px.colors.sequential.dense_r,
                        hover_name="districts")
        st.plotly_chart(fig_amount)



        
    elif question == "10. Registered users of Top User":
        query10 = '''SELECT States,SUM(RegisteredUsers) AS RegisteredUsers
                        FROM top_user group BY States ORDER BY RegisteredUsers DESC LIMIT 10;'''
        mycursor.execute(query10)
        table=mycursor.fetchall()
        mydb.commit()

        df_10 = pd.DataFrame(table, columns=['States',  'RegisteredUsers'])
        fig_amount = px.bar(df_10, x="States", y="RegisteredUsers", title="RegisteredUsers",color_discrete_sequence= px.colors.sequential.Viridis,
                        hover_name="States")
        st.plotly_chart(fig_amount)
        