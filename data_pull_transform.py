from datetime import datetime, date, timedelta
import zipfile
import streamlit as st
from urllib.request import urlopen
import pandas as pd
import numpy as np
import yfinance as yf
import logging
import re
import ssl
import plotly.express as px
from GoogleNews import GoogleNews
import os
import plotly.graph_objects as go

# LOGGING
logging.basicConfig(filename='data_pull.log', level=logging.INFO)

# SSL Error fix for IFW Kiel
ssl._create_default_https_context = ssl._create_unverified_context

# --- GLOBALS
END_DATE = date.today() - timedelta(days=1)
START_DATE = END_DATE - timedelta(days=365)
BG_COLOR = '#fafafa'
GRAPH_SCHEME = 'ggplot2'
COLOR_SEQUENCE = ['#c98b2d', '#152c44', '#919daa', '#931d1d', '#e9e8e4', '#5e6063', '#c1997c', '#e8cd90', '#3e74c4']
ACLED_DATA = r'https://acleddata.com/download/38560/?tmstv=1673161723'
STORAGE_OPTIONS = {'User-Agent': 'Mozilla/9.0'}
TARGET_FOLDER = 'assets'
DATA_SOURCES = 'data_sources.xlsx'

# --- EMBEDDINGS
UNHCR_APP = 'https://app.powerbi.com/view?r=eyJrIjoiZDBlM2EwOWMtMDk2Mi00ZDc4LTliYWUtZTNjMmNlN2ZmY2Y4IiwidCI6ImU1YzM3OTgxLTY2NjQtNDEzNC04YTBjLTY1NDNkMmFmODBiZSIsImMiOjh9'

# --- YAHOO FINANCE TEST PARAMETERS
INSTRUMENT_LIST = {
    'code': ['UAH=X'],
    'label': ['UAH/USD'],
    'type': ['FX rate']
}


# --- UNLOCK FOR PRODUCTION
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 150)

# --- MAPBOX TOKEN
def get_mapbox_token(label = 'MAPBOX_TOKEN'):
    try:
        return os.environ[label]
    except Exception as e:
        logging.error(f'Could not export Mapbox token {e}')
        return None

# --- TEST FUNCTIONS
def log_data_transform(output):
    now = datetime.now()
    current_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    logging.info(f'{output} stored successfully: {current_time}')

# --- GOOGLE NEWS
def convert_to_link(link, cover_text='Link'):
    return f'[{cover_text}](https://{link})'

def get_google_news(lang='en', region='US', search_topic='Ukraine', output=f'{TARGET_FOLDER}/tf_google_news.csv'):
    try:
        googlenews = GoogleNews(lang=lang, region=region)
        news = googlenews.get_news(search_topic)
        news = googlenews.results()
        df = pd.DataFrame(news)
        df = df[['title', 'media', 'date', 'link']]
        df['title'] = df['title'].str.replace('More', ': ')
        df.columns = ['Title', 'Media', 'Date', 'Link']
        df['Link'] = df['Link'].apply(convert_to_link)
        df.to_csv(output, encoding = 'utf-16', index=False)
        log_data_transform(output=output)
        return df
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

def plot_google_news(source = f'{TARGET_FOLDER}/tf_google_news.csv'):
    df = pd.read_csv(source, encoding = 'utf-16')
    tab = go.Figure(data=[go.Table(
    header=dict(values=list(df.columns),
                #fill_color='paleturquoise',
                align='left'
                ),
    cells=dict(values=[df.Title, df.Media, df.Date, df.Link],
               #fill_color='lavender',
               align='left'
               ))
    ])
    tab.update_layout(height=400)
    return tab

# --- GDP World Bank
def get_gdp_ua(source='https://api.db.nomics.world/v22/series/WB/WDI/A-NY.GDP.MKTP.CD-UKR.csv', output=f'{TARGET_FOLDER}/tf_gdp_ua.csv'):
    try:
        df = pd.read_csv(source)
        df.columns = ['Year', 'Value']
        df['Value'] = df['Value']/(10**9)
        df['Series'] = 'Annual GDP, current USD bn'
        now = datetime.now()
        current_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        df['Retrieved on'] = current_time
        df.to_csv(output, encoding='utf-16')
        return df
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

# --- YAHOO FINANCE
def get_yf_instrument(instrument, alias, type, start_date, end_date):
    try:
        df = yf.download(instrument, start=start_date, end=end_date)
        df = df[["Adj Close"]]
        df["date"] = df.index
        if alias is None:
            df["instrument"] = instrument
        else:    
            df["instrument"] = alias
        df["type"] = type
        df["value"] = df["Adj Close"]
        df = df[["date", "type", "instrument", "value"]]
        return df
    except Exception as e:
        logging.error("Unable to retrieve the currency pair {fx}", exc_info=True)   

def get_yf_data(currency_list=INSTRUMENT_LIST, output = f'{TARGET_FOLDER}/tf_yf_data.csv', start_date = START_DATE, end_date = END_DATE):
    try:
        length = len(currency_list['code'])
        df = pd.DataFrame()
        for c in range(0,length):
            df_temp = get_yf_instrument(currency_list['code'][c], currency_list['label'][c], currency_list['type'][c], start_date, end_date)
            df = df.append(df_temp)   
        df.to_csv(output, index=False, encoding = 'utf-16')
        log_data_transform(output=output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

def plot_ccy_data(source = f'{TARGET_FOLDER}/tf_yf_data.csv', instrument = 'UAH/USD', title = 'FX rate', retrieved_from='Yahoo Finance'):
    df = pd.read_csv(source,  encoding = 'utf-16')
    df_plot = df[df['instrument']==instrument]
    fig = px.area(df, x = df_plot['date'], y = 'value',
        title=f'{title} <br>Source: {retrieved_from}</br>',
        labels={
            'x': 'Date',
            'date': 'Date',
            'value': instrument
        }
    )
    return fig

# --- MANUAL DATA
def get_ua_data(source = f'{TARGET_FOLDER}/{DATA_SOURCES}', output = TARGET_FOLDER):
    # Parse list of data sources
    df = pd.read_excel(source)
    #  Parse and store the files
    for ind in df.index:
        dactive = df['active'][ind]   
        if dactive == 1:
            dext = df['extension'][ind]
            dlink = df['link'][ind]
            dskip = df['row skip'][ind]
            dfunction = df['function'][ind]
            dsheet_count = df['sheet_count'][ind]
            print(dlink)
            print(dsheet_count)
            if dext == 'csv':
                try:
                    df_return = pd.read_csv(dlink)
                except Exception as e:
                    logging.error(f'Could not download {dlink}: {e}')
            elif dext == 'xlsx':
                dsheet = str(df['sheet'][ind])
                if dsheet == '' or pd.isna(dsheet) == True or dsheet == 'nan':
                    dsheet = int(df['sheet_count'][ind])
                    if np.isnan(dsheet) == True:
                        dsheet = 0
                try:
                    df_return = pd.read_excel(dlink, sheet_name=dsheet, header=0, skiprows=dskip, storage_options=STORAGE_OPTIONS)
                except:
                    try:
                        df_return = pd.read_excel(dlink, sheet_name=dsheet, header=0, skiprows=dskip)
                    except:
                        df_return = pd.read_excel(dlink, sheet_name=int(dsheet_count), header=0, skiprows=dskip)
            elif dext == 'zip':
                df_return = pd.read_csv(dlink, compression='zip')
            else:
                pass
            now = datetime.now()
            current_time = now.strftime("%m/%d/%Y, %H:%M:%S")
            df_return['retrieved'] = current_time
            df_return.to_csv(f'{output}/src_{dfunction}.csv', index=False, encoding='utf-16')      
            log_data_transform(dfunction) 

# --- GRAIN FUNCTIONS
def transform_grain_data(source = f'{TARGET_FOLDER}/src_grain_destinations.csv', output=f'{TARGET_FOLDER}/tf_grain_destinations.csv'):
    try:
        df = pd.read_csv(source, thousands=r',', encoding='utf-16')
        df['Income group'] = df['Income group'].fillna('mixed')
        df = df[['Country', 'Income group', 'Tonnage']]
        df = df.groupby(['Country', 'Income group']).sum('Tonnage')
        df = df.sort_values(by=['Tonnage'], ascending=False)
        df = df.reset_index()
        df.columns = ['Country', 'Income group', 'Tons received']
        df.to_csv(output, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not transform {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_grain_destinations(source=f'{TARGET_FOLDER}/tf_grain_destinations.csv', title ='Grain delivered under grain deal', retrieved_from='WFO | HDX'):
    df = pd.read_csv(source, encoding='utf-16')
    fig = px.bar(df, x = 'Tons received', y='Income group', color = 'Country', orientation='h',
        hover_data={'Tons received': ':.0f'},
        text_auto='.2s',
        color_discrete_sequence=COLOR_SEQUENCE,
        title=f'{title} <br>Source: {retrieved_from}</br>',
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    return fig

# --- HUMANITARIAN DATA
def transform_hum_data(source = f'{TARGET_FOLDER}/src_hum_data.csv', output=f'{TARGET_FOLDER}/tf_hum_data.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        df = df[df.iloc[:, 0] != '#population+total']
        df = df[['People Affected(Flash Appeal)', 'IDPs', 'Refugees(UNHCR)', 'Civilian casualities(OHCHR) - Killed', 'Civilian casualities(OHCHR) - Injured', 'Attacks on Education Facilities', 'Attacks on Health Care', 'Date']]
        df.columns = ['People affected', 'Internally Displaced', 'Refugees', 'Civilian deaths, confirmed', 'Civilians injured, confirmed', 'Attacks on Education Facilities', 'Attacks on Health Care', 'Date']
        df = df.fillna(method='ffill')
        df.to_csv(output, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_hum_data(source=f'{TARGET_FOLDER}/tf_hum_data.csv', series = 'Refugees', title = 'Refugee count', retrieved_from='UNHCR | HDX'):
    df = pd.read_csv(source, encoding='utf-16')
    fig = px.area(df, y = series, x = 'Date', title=f'{title} <br>Source: {retrieved_from}</br>',)
    fig.update_layout(xaxis={'visible': True, 'showticklabels': True}, yaxis={'visible': True, 'showticklabels': True})
    return fig

# --- RECONSTRUCTION & DAMAGE & REGIONS
def transform_reconstruction_sectors(source=f'{TARGET_FOLDER}/src_reconstruction_sectors.csv' ,  output=f'{TARGET_FOLDER}/tf_reconstruction_sectors.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        df.to_csv(output, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_reconstruction_sectors(source=f'{TARGET_FOLDER}/tf_reconstruction_sectors.csv', series = 'Damage', title = 'Damage assessment as of August 2022', retrieved_from='World Bank (2022)'):
    df = pd.read_csv(source, encoding='utf-16')
    fig = px.treemap(df, path=[px.Constant("All"), 'Sector Type', 'Sector'], values=series,
        color_discrete_sequence=COLOR_SEQUENCE,
        title=f'{title} <br>Source: {retrieved_from}</br>')
    fig.update_layout(xaxis={'visible': False, 'showticklabels': True}, yaxis={'visible': False, 'showticklabels': True})
    return fig

def transform_reconstruction_regions(source=f'{TARGET_FOLDER}/src_reconstruction_regions.csv' ,  output=f'{TARGET_FOLDER}/tf_reconstruction_regions.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        df = df[df['Oblast'].isin(['Support regions, subtotal','Backline regions, subtotal','Regions where government has regained control, subtotal'])!=True]
        df.to_csv(output, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_reconstruction_regions(source=f'{TARGET_FOLDER}/tf_reconstruction_regions.csv', title = 'Damage by regions as of August 2022', retrieved_from='World Bank (2022)'):
    df = pd.read_csv(source, encoding='utf-16')
    fig = px.bar(df, x = 'Damage', y='Oblast', orientation='h', color = 'Oblast type',
        text_auto='.2s',
        hover_data={'Damage': ':.1f'},
        color_discrete_sequence=COLOR_SEQUENCE,
        title=f'{title} <br>Source: {retrieved_from}</br>',
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(legend=dict(orientation="h", y=-0.25))
    return fig

# --- UKRAINE SUPPORT
def transform_support_data(source=f'{TARGET_FOLDER}/src_ukraine_support.csv' ,  output=f'{TARGET_FOLDER}/tf_ukraine_support.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        df = df[['Countries',	
            'Announcement Date',	
            'Type of Aid General',
            'Value Committed (own estimate, in USD)',	
            'Value Delivered (own estimate, in USD)',	
            'Converted Value in EUR',
            'Total monetary value delivered in EUR', 
            'retrieved']]
        df = df.replace('.', np.nan)
        df['Value committed'] = df['Converted Value in EUR']
        # df.loc[df['Value committed'].isna() == True, 'Value committed'] = df['Value committed (own estimate, in USD)']
        df['Value delivered'] = df['Total monetary value delivered in EUR']
        # df.loc[df['Value delivered'].isna() == True, 'Value delivered'] = df['Value delivered (own estimate, in USD)']
        # df.loc[df['Value delivered'].isna() == True, 'Value delivered'] = 0
        df = df[(df['Value committed'] != 'No price')]
        df = df[(df['Value delivered'] != 'No price')]
        df['Value committed'] = df['Value committed'].astype(float) / 10**9 #bn USD
        df['Value delivered'] = df['Value delivered'].astype(float) / 10**9 #bn USD
        df = df.groupby(['Countries', 'Type of Aid General', 'retrieved']).agg({'Value committed':'sum','Value delivered':'sum'}).reset_index()
        df['Ratio: Delivered to committed'] = df['Value delivered']/df['Value committed']
        df.rename(columns={'Countries': 'countries'}, inplace=True)
        df.columns = ['countries', 'Type of Aid General', 'retrieved', 'Value committed', 'Value delivered', 'Ratio: Delivered to committed']
        df = df.reset_index()
        df.to_csv(output, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_ukraine_support(source=f'{TARGET_FOLDER}/tf_ukraine_support.csv', series = 'Value committed', title='Public commitment to support Ukraine (both cash and kind)', retrieved_from='IFW Kiel'):
    df = pd.read_csv(source, encoding='utf-16')
    as_of_date = df['retrieved'].iloc[0]
    fig = px.treemap(df, path=[px.Constant("All"), 'Type of Aid General','countries'], values=series,
                hover_data={series: ':.2f'},
                color_discrete_sequence=COLOR_SEQUENCE,
                title=f'{title} USDbn <br>Source: {retrieved_from}</br>'
            )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(xaxis={'visible': False, 'showticklabels': True}, yaxis={'visible': False, 'showticklabels': True})
    return fig

def plot_delivery_rate(source=f'{TARGET_FOLDER}/tf_ukraine_support.csv', title = 'Declared support and delivery rate, top 10 by commitment', retrieved_from='IFW Kiel'):
    df = pd.read_csv(source, encoding='utf-16')
    as_of_date = df['retrieved'].iloc[0]
    df_plot = df.groupby(['countries']).agg({'Value committed': 'sum', 'Value delivered': 'sum'}).reset_index()
    df_plot['Ratio: Delivered to committed'] = df_plot['Value delivered'] / df_plot['Value committed']
    df_plot = df_plot.sort_values(by='Value committed', ascending=False)
    df_plot = df_plot.iloc[:10,]
    fig = px.bar(df_plot, y="countries", 
                    x="Value committed",
                    color='Ratio: Delivered to committed', 
                    orientation='h', 
                    text_auto='.2s',
                    hover_data={'Ratio: Delivered to committed': ':.1f'},
                    title=f'{title} USDbn <br>Source: {retrieved_from}</br>',
                    color_discrete_sequence=COLOR_SEQUENCE)
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(legend=dict(orientation="h"))
    return fig

# --- FISCAL AND FINANCIAL DATA
def clean_fiscal_data(df_source, source_file, sheet_labels):
    try:
        df = df_source
        df_labels = pd.read_excel(source_file, sheet_name=sheet_labels)
        
        # Add labels
        df_labels = pd.read_excel(source_file, sheet_name = sheet_labels)
        df = pd.concat([df, df_labels], axis=1)

        # Clean-up
        df = df[df['active']==1]
        df = df.dropna(how='all', axis=1)
        df = df.iloc[:,-6:]
        last_date = list(df)[0]
        df['date'] = last_date[:11]
        df.columns = ['Value', 'Retrieve date', 'Item', 'Code', 'Active', 'Total', 'Date']

        # Calculate shares
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        total_value = df[df['Total']==1]['Value'].to_list()[0]
        df['Share'] = df['Value']/total_value
        df = df[df['Total']==0]
        df = df.drop(['Total'], axis=1)
        return df
    except Exception as e:
        logging.error(f'Could not clean fiscal data. Error: {e}')

def transform_fiscal_income(source=f'{TARGET_FOLDER}/src_fiscal_income.csv' ,  output=f'{TARGET_FOLDER}/tf_fiscal_income.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        df = clean_fiscal_data(df, f'{TARGET_FOLDER}/{DATA_SOURCES}', sheet_labels='labels_fiscal_income')
        df = df.reset_index()
        df.to_csv(output, index=False, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_fiscal_income(source=f'{TARGET_FOLDER}/tf_fiscal_income.csv', retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    as_of_date = df['Date'][0]
    fig = px.bar(df, x = 'Value', y='Item', orientation='h',
        text_auto='.2s',
        hover_data={'Share': ':.1f'},
        color_discrete_sequence=COLOR_SEQUENCE,
        title=f"General Government Income as of {as_of_date} <br>Source: {retrieved_from}</br>"
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(xaxis={'visible': False, 'showticklabels': True}, yaxis={'visible': True, 'showticklabels': True})
    return fig

def transform_fiscal_expenses(source=f'{TARGET_FOLDER}/src_fiscal_expenses.csv' ,  output=f'{TARGET_FOLDER}/tf_fiscal_expenses.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        df = clean_fiscal_data(df, f'{TARGET_FOLDER}/{DATA_SOURCES}', sheet_labels='labels_fiscal_expenses')
        df = df.reset_index()
        df.to_csv(output, index=False, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_fiscal_expenses(source=f'{TARGET_FOLDER}/tf_fiscal_expenses.csv', retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    as_of_date = df['Date'][0]
    fig = px.bar(df, x = 'Value', y='Item', orientation='h',
        text_auto='.2s',
        hover_data={'Share': ':.1f'},
        color_discrete_sequence=COLOR_SEQUENCE,
        title=f"General Government Expenses {as_of_date} <br>Source: {retrieved_from}</br>"
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(xaxis={'visible': False, 'showticklabels': True}, yaxis={'visible': True, 'showticklabels': True})
    return fig

def transform_fiscal_finance(source=f'{TARGET_FOLDER}/src_fiscal_finance.csv' ,  output=f'{TARGET_FOLDER}/tf_fiscal_finance.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        df = clean_fiscal_data(df, f'{TARGET_FOLDER}/{DATA_SOURCES}', sheet_labels='labels_fiscal_finance')
        df = df.reset_index()
        df.to_csv(output, index=False, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_fiscal_finance(source=f'{TARGET_FOLDER}/tf_fiscal_finance.csv', retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    as_of_date = df['Date'][0]
    fig = px.bar(df, x = 'Value', y='Item', orientation='h',
        text_auto='.2s',
        hover_data={'Share': ':.1f'},
        color_discrete_sequence=COLOR_SEQUENCE,
        title=f"General Government Deficit Finance Source as of {as_of_date} <br>Source: {retrieved_from}</br>"
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(xaxis={'visible': False, 'showticklabels': True}, yaxis={'visible': True, 'showticklabels': True})
    return fig

def transform_cpi_headline(source=f'{TARGET_FOLDER}/src_cpi_headline.csv',  output_last=f'{TARGET_FOLDER}/tf_cpi_last.csv', output_12m=f'{TARGET_FOLDER}/tf_cpi_12m.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')

        # Add labels
        df_labels = pd.read_excel(f'{TARGET_FOLDER}/{DATA_SOURCES}', sheet_name = 'labels_cpi_headline')
        df = pd.concat([df, df_labels], axis=1)
        df = df.dropna(how='all', axis=1)
        
        # Export the last inflation
        # Clean-up
        df_last = df.iloc[:,-4:]
        df_last = df_last.dropna()
        last_date = list(df_last)[0]
        df_last['date'] = last_date[:11]
        df_last.columns = ['Value', 'Retrieve date', 'Item', 'Total', 'Date']
        df_last = df_last.reset_index()
        df_last.to_csv(output_last, index=False, encoding='utf-16')
        log_data_transform(output_last)
    except Exception as e:
        logging.error(f'Could not get {output_last}. Error: {e}')

    try:
        # Export the 12m inflation
        # Clean-up
        select_label = 'Inflation, yoy'
        df_12m = df.iloc[:,-15:]
        df_12m = df_12m[df['column_name']==select_label]
        retrieved = df_12m['retrieved'].iloc[0]
        df_12m = df_12m.drop(['retrieved', 'total', 'column_name'], axis=1)
        df_12m = df_12m.T
        df_12m.columns = [select_label]
        df_12m['Date'] = df_12m.index
        df_12m['Retrieved'] = retrieved
        df_12m = df_12m.reset_index()
        df_12m.to_csv(output_12m, index=False, encoding='utf-16')
        log_data_transform(output_12m)
    except Exception as e:
        logging.error(f'Could not get {output_12m}. Error: {e}')


def plot_cpi_last(source=f'{TARGET_FOLDER}/tf_cpi_last.csv', retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    as_of_date = df['Date'][0]
    fig = px.bar(df, x = 'Value', y='Item', orientation='h',
        text_auto='.2s',
        hover_data={'Value': ':.1f'},
        color_discrete_sequence=COLOR_SEQUENCE,
        color='Total',
        title=f"Inflation by components as of {as_of_date} <br>Source: {retrieved_from}</br>",
        labels={'Value': '%'}
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(showlegend=False)
    fig.update_layout(xaxis={'visible': True, 'showticklabels': True}, yaxis={'visible': True, 'showticklabels': True})
    return fig

@st.cache_resource(ttl = '7 days')
def plot_cpi_12m(source=f'{TARGET_FOLDER}/tf_cpi_12m.csv', series = 'Inflation, yoy', retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    as_of_date = df['Retrieved'][0]
    fig = px.area(df, x = 'Date', y = series,
        hover_data={series: ':.1f'},
        title=f"{series} <br>Source: {retrieved_from}</br>",
        labels={series: '%'}
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(showlegend=False)
    fig.update_layout(xaxis={'visible': True, 'showticklabels': True}, yaxis={'visible': True, 'showticklabels': True})
    return fig

def transform_international_reserves(source=f'{TARGET_FOLDER}/src_international_reserves.csv' ,  output=f'{TARGET_FOLDER}/tf_international_reserves.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        
        # Add labels
        df_labels = pd.read_excel(f'{TARGET_FOLDER}/{DATA_SOURCES}', sheet_name = 'labels_international_reserves')
        df = pd.concat([df, df_labels], axis=1)
        # Clean-up
        df = df.dropna(how='all', axis=1)
        df = df.iloc[:,-4:]
        df = df.dropna()
        last_date = list(df)[0]
        last_date = re.sub(r"\d+", "", last_date)
        df['date'] = last_date[:11]
        df.columns = ['Value', 'Retrieve date', 'Item', 'Total', 'Date']
        df = df.reset_index()
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')/1000
        reserves_total = df[df['Total']==True]['Value'].to_list()[0]
        df['Share'] = round(df['Value'] / reserves_total, 2)*100
        df.to_csv(output, index=False, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_international_reserves(source=f'{TARGET_FOLDER}/tf_international_reserves.csv', title = 'International reserves, bn USD', retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    as_of_date = df['Date'][0]
    df = df[df['Total']!=True]
    fig = px.bar(df, x = 'Value', y='Item', orientation='h',
        text_auto='.2s',
        hover_data={'Share': ':.1f'},
        color_discrete_sequence=COLOR_SEQUENCE,
        title=f"{title} as of {as_of_date} <br>Source: {retrieved_from}</br>"
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(showlegend=False)
    fig.update_layout(xaxis={'visible': False, 'showticklabels': True})
    # fig.update_layout(template='plotly_white')
    return fig

def transform_bond_yields(source=f'{TARGET_FOLDER}/src_bond_yields.csv', output=f'{TARGET_FOLDER}/tf_bond_yields.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        # Add labels
        df_labels = pd.read_excel(f'{TARGET_FOLDER}/{DATA_SOURCES}', sheet_name = 'labels_bond_yields')
        df_labels['index'] = df_labels.index
        df_column_list = df_labels['column_name'].to_list()
        df_column_list.append('Retrieved on')
        df.columns = df_column_list
        
        # Drop unneccessary columns
        drop_columns = df_labels['index'][df_labels['active']==0]
        df = df.drop(df.columns[drop_columns], axis = 1)
        df = df.dropna(how='any', axis=0)
        df = df.reset_index()
        df.to_csv(output, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_bond_yields(source=f'{TARGET_FOLDER}/tf_bond_yields.csv', title = "Bond Placements and Their Yields, UAH mn", retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    df_plot = df[df['month']!='Total for the year 2022']
    fig = px.bar(df_plot, x = 'month', y='UAH: amount',
        color='UAH: weighted yield',
        text_auto='.2s',
        hover_data={'UAH: amount': ':.1f'},
        title=f"{title} <br>Source: {retrieved_from}</br>",
        labels={'month': ''}
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    fig.update_layout(coloraxis=dict(colorbar=dict(orientation='h', y=-1)))
    # fig.update_coloraxes(colorbar_orientation="h")
    # fig.update_layout(layout_coloraxis_showscale=False)
    # fig.update_coloraxes(showscale=False)
    # fig.update_layout(template='plotly_white')
    return fig

def transform_policy_rate(source=f'{TARGET_FOLDER}/src_policy_rate.csv' ,  output=f'{TARGET_FOLDER}/tf_policy_rate.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        # Add labels
        df_labels = pd.read_excel(f'{TARGET_FOLDER}/{DATA_SOURCES}', sheet_name = 'labels_policy_rate')
        df_labels['index'] = df_labels.index
        df_column_list = df_labels['column_name'].to_list()
        df_column_list.append('Retrieved on')
        df.columns = df_column_list
        
        # Drop unneccessary columns
        drop_columns = df_labels['index'][df_labels['active']==0]
        df = df.drop(df.columns[drop_columns], axis = 1)
        df = df.dropna(how='any', axis=0)
        df = df.reset_index()
        df.to_csv(output, encoding='utf-16')
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

def plot_policy_rate(source=f'{TARGET_FOLDER}/tf_policy_rate.csv', title = "Policy rate dynamics, %", retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    df_plot = df
    if len(df_plot) < 3:
        fig = px.bar(df_plot, x = 'Date', y='Reference rate',
            hover_data={'Reference rate'},
            text_auto='.2s',
            title=f'{title} <br>Source: {retrieved_from}</br>'
        )
    else:
        fig = px.area(df_plot, x = 'Date', y='Reference rate',
            hover_data={'Reference rate'},
            title=f'{title} <br>Source: {retrieved_from}</br>'
        ) 
    return fig

def transform_interest_rates(source=f'{TARGET_FOLDER}/src_interest_rates.csv' ,  output=f'{TARGET_FOLDER}/tf_interest_rates.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
        # Add labels
        df_labels = pd.read_excel(f'{TARGET_FOLDER}/{DATA_SOURCES}', sheet_name = 'labels_interest_rates')
        df_labels['index'] = df_labels.index
        df_column_list = df_labels['column_name'].to_list()
        df_column_list.append('Retrieved on')
        df.columns = df_column_list
        
        # Drop unneccessary columns
        drop_columns = df_labels['index'][df_labels['active']==0]
        df = df.drop(df.columns[drop_columns], axis = 1)
        df = df[(df['Region'] != 'including') & (df['Region'] != 'including by currencies')]  # Drops rates by maturities
        df = df.dropna(how='any', axis=0)
        df = df.reset_index()
        df = df.drop(['index'], axis=1)
        
        # Convert all columns to numeric except for calculations
        cols = df.columns.drop(['Region', 'Retrieved on'])
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
        df['Spread: households to non-financial corporations'] = df['Nationals: households'] - df['Nationals: non-financial corporations']

        #Export
        df.to_csv(output, encoding='utf-16', index=False)
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_interest_rates(source=f'{TARGET_FOLDER}/tf_interest_rates.csv', retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16')
    df_plot = df
    as_of_date = df_plot['Retrieved on'].iloc[0]
    df_plot.loc[df_plot['Region'] == 'Autonomous Republic of Crimea and the city of Sevastopol', 'Region'] = 'Crimea and Sevastopol' 
    fig = px.bar(df_plot, x = 'Nationals: average', y='Region', orientation='h',
        text_auto='.2s',
        hover_data={'Nationals: average': ':.1f'},
        color_discrete_sequence=COLOR_SEQUENCE,
        title=f"Lending rates by region (only nationals), in % as of {as_of_date} <br>Source: {retrieved_from}</br>"
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, template = GRAPH_SCHEME)
    return fig

def transform_financial_soundness(source=f'{TARGET_FOLDER}/src_financial_soundness.csv' ,  output=f'{TARGET_FOLDER}/tf_financial_soundness.csv'):
    try:
        df = pd.read_csv(source, encoding='utf-16')
       
        # Clean the first column
        df['Item'] = (df['Indicator']
                           .str.replace(r'[^a-zA-Z\s]', '')
                           .str.strip()
                           .apply(lambda x: re.sub(r'\s+', ' ', x))
                           )

        # Clean data
        df = (df[df['Item'].isna()==False]
              .drop(columns=['retrieved'])
              )
        Item_list = [
            'Tier capital to riskweighted assets', 
            'Nonperforming loans net of provisions to capital', 
            'Nonperforming loans to total gross loans', 
            'Net open position in foreign exchange to capital'
            ] 
        # df = df[df['Item'].isin(Item_list)]
        column_list_t = df['Item'].to_list()
        df = df.T
        df.columns = column_list_t
        df = df.drop(df.index[0])
        df = df.apply(pd.to_numeric, errors='coerce')
        df['Retrieved'] = date.today()
        df = df.reset_index()
        df.to_csv(output, encoding='utf-16', index=False)
        log_data_transform(output)
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_financial_soundness(source=f'{TARGET_FOLDER}/tf_financial_soundness.csv', series='Nonperforming loans net of provisions to capital', retrieved_from='National Bank of Ukraine'):
    df = pd.read_csv(source, encoding='utf-16', index_col='index')
    df_plot = df.iloc[-12:,]
    if series == 'Net open position in foreign exchange to capital':
        fig = px.bar(df_plot, x = df_plot.index, y=series,
            hover_data={series: ':.1f'},
            title=f"{series}, in % <br>Source: {retrieved_from}</br>",
            text_auto='.2s',
            labels={
                'index': 'Date Quarter',
                'x': 'Date Quarter'
                }
        )
    else:
        fig = px.area(df_plot, x = df_plot.index, y=series,
            hover_data={series: ':.1f'},
            title=f"{series}, in % <br>Source: {retrieved_from}</br>",
            labels={
                'index': 'Date Quarter',
                'x': 'Date Quarter'
                }
        )
    fig.update_layout(template = GRAPH_SCHEME)
    return fig

# --- ACLED DATA WITH FATALITIES
def get_fatalities(source = 'https://acleddata.com/download/38560/?tmstv=1673161723', output = f'{TARGET_FOLDER}/src_fatalities.csv.gz', sheet_name = 0, storage_options=STORAGE_OPTIONS):
    try:
        df = pd.read_excel(source,  sheet_name=sheet_name, storage_options=storage_options)
        now = datetime.now()
        current_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        df['retrieved'] = current_time
        df.to_csv(output, index=False, encoding = 'utf-16', compression='gzip')
        print(f"Data for fatalities retrieved at {current_time}") 
    except Exception as e:
        logging.error(f'Could not get {output}. Error: {e}')

def transform_fatalities(source = f'{TARGET_FOLDER}/src_fatalities.csv.gz', output_geo = f'{TARGET_FOLDER}/tf_fatalities_geo.csv.gz', output_fatalities = f'{TARGET_FOLDER}/tf_fatalities_series.csv'):
    try:
        df = pd.read_csv(source,  encoding = 'utf-16', compression='gzip')
        df = df[df['EVENT_DATE'] > '2022-02-23']
        df['DATE'] = pd.to_datetime(df['EVENT_DATE'], format='%Y-%m-%d')
        df['WEEK'] = df['DATE'].dt.isocalendar().week.astype('str')
        df['MONTH'] = df['DATE'].dt.month.astype('str')
        df['YEAR'] = df['DATE'].dt.year.astype('str')
        df['WEEK_DATE'] = df['YEAR'] + '-' + df['WEEK']
        df['MONTH_DATE'] = df['YEAR'] + '-' + df['MONTH']
        df['SIZE'] = 10 + df['FATALITIES'] * 1
        df['COUNT'] = 1
        df = df.reset_index()
        df_geo = df
        df_geo.to_csv(output_geo, encoding='utf-16', index=False, compression="gzip")
        df_fatalities = df.groupby(['MONTH_DATE', 'ACTOR1', 'EVENT_TYPE'])['FATALITIES'].sum()
        df_fatalities = df.reset_index()
        df_fatalities.to_csv(output_fatalities, encoding='utf-16')
        log_data_transform(output_geo)
        log_data_transform(output_fatalities)
    except Exception as e:
        logging.error(f'Could not get {output_geo} and {output_fatalities}. Error: {e}')

@st.cache_resource(ttl = '7 days')
def plot_fatalities_geo(source = f'{TARGET_FOLDER}/tf_fatalities_geo.csv.gz', mapbox_token = get_mapbox_token(), title='Conflict events, daily', retrieved_from='ACLED'):
    df = pd.read_csv(source,  encoding = 'utf-16', compression = 'gzip')
    df['DATE'] = pd.to_datetime(df['DATE'])
    df_plot = df.sort_values(by=['DATE'])
    px.set_mapbox_access_token(mapbox_token)
    fig = px.scatter_mapbox(df_plot,
        animation_frame='DATE',
        # animation_group='EVENT_TYPE',
        lat="LATITUDE", 
        lon="LONGITUDE", 
        color="EVENT_TYPE", 
        size="SIZE",
        hover_data={'FATALITIES': ':.1f'},
        title=f'{title} <br>Source: {retrieved_from}</br>',
        labels={
            "MONTH_DATE": "Month date",
            "EVENT_TYPE": "Event type",
            "FATALITIES": "Reported deaths",
            "COUNT": "Number of conflict actions"
        },
        color_continuous_scale=px.colors.sequential.Sunsetdark, 
        size_max=50, 
        zoom=5,
        height=600)
    fig.update_layout(legend=dict(orientation="h", y=-0.02))
    return fig

@st.cache_resource(ttl = '7 days')
def plot_fatalities_series(source = f'{TARGET_FOLDER}/tf_fatalities_series.csv', series = 'FATALITIES', title = 'FATALITIES', retrieved_from='ACLED'):
    df = pd.read_csv(source,  encoding = 'utf-16')
    df_plot = df.groupby(['MONTH_DATE', 'EVENT_TYPE'])[series].sum()
    df_plot = df_plot.reset_index()
    fig = px.bar(df_plot, 
        x = 'MONTH_DATE', 
        y = series,
        color='EVENT_TYPE',
        hover_data={f'{series}': ':.0f'},
        title=f"{title} by month <br>Source: {retrieved_from}</br>",
        labels={
            "MONTH_DATE": "Month date",
            "EVENT_TYPE": "Event type",
            "FATALITIES": "Reported deaths",
            "COUNT": "Number of conflict actions"
        },
    )
    fig.update_layout(template = GRAPH_SCHEME)
    fig.update_layout(xaxis={'visible': False, 'showticklabels': True})
    fig.update_layout(legend=dict(orientation="h"))
    return fig

def get_data():
    get_ua_data()
    get_google_news()
    get_yf_data()
    # get_fatalities()
    get_gdp_ua()
    print('All source data retrieved successfully')

def transform_data():
    transform_hum_data()
    transform_grain_data()
    transform_reconstruction_sectors()
    transform_reconstruction_regions()
    transform_support_data()
    transform_fiscal_income()
    transform_fiscal_expenses()
    transform_fiscal_finance()
    transform_cpi_headline()
    transform_international_reserves()
    transform_bond_yields()
    transform_policy_rate()
    transform_interest_rates()
    transform_financial_soundness()
    # transform_fatalities()
    print('All data transformed successfully')

def process_data(get_source = True, transform = True):
    if get_source == True:
        get_data()
    
    if transform == True:
        transform_data()

def main():
    # Full
    # process_data(get_source=True, transform=True)
    # For testing
    # get_google_news()
    # plot_ccy_data().show()
    # plot_hum_data(series = 'Refugees', title='Refugees').show()
    # plot_hum_data(series = 'Internally Displaced', title='Internally Displaced').show()
    # plot_hum_data(series = 'Civilian deaths, confirmed', title='Civilian deaths, confirmed').show()
    # plot_hum_data(series = 'Civilians injured, confirmed', title='Civilians injured, confirmed').show()
    # plot_reconstruction_sectors(series = 'Damage', title = 'Damage assessment as of August 2022, USD bn').show()
    # plot_reconstruction_sectors(series = 'Needs', title = 'Reconstruction needs assessment as of August 2022, USD bn').show()
    # plot_reconstruction_regions().show()
    # plot_ukraine_support(series='Value committed', title = 'Support publicly announced, USD bn').show()
    # plot_ukraine_support(series='Value delivered', title = 'Support delivered in cash and kind, USD bn').show()
    # plot_cpi_headline().show()
    # plot_international_reserves().show()
    # plot_bond_yields().show()
    # plot_policy_rate().show()
    # plot_interest_rates().show()
    # plot_financial_soundness(series='Nonperforming loans net of provisions to capital').show()
    # plot_financial_soundness(series='Liquid assets to total assets').show()
    # plot_fatalities_series(series = 'FATALITIES', title = 'Number of Fatalities').show()
    # plot_fatalities_series(series='COUNT', title = 'Number of conflict events').show()
    # plot_fatalities_geo().show()
    # plot_grain_destinations().show()
    # plot_cpi_12m().show()
    plot_cpi_last().show()

if __name__ == "__main__":
    main()

