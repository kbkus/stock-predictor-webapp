import streamlit as st
from datetime import datetime
from functions import scrape_finviz, get_stock_prices

def merge_datetime(row):
    return datetime.strptime(row['date']+row['time'], '%b-%d-%y%I:%M%p')

@st.cache
def get_data(company):
    df = scrape_finviz(company, status=False)
    df = get_stock_prices(df)
    df['datetime'] = df.apply(merge_datetime, axis=1)
    return df



if __name__ == '__main__':
    company = st.text_input('Type in stock ticker')
    if company:
        df = get_data([company])
        st.write(f'{company} headlines scraped from finviz')
        st.write(df[['company','headline','date']])
        line_chart = st.line_chart(df[['datetime', 'open_price', 'close']].set_index('datetime'))
        
        
        # work on date range slider
        #range = st.sidebar.slider('Price Range', 0, int(df['open_price'].max()+100), (0, int(df['open_price'].max())))
