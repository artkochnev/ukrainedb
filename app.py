from datetime import date, timedelta
import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import plotly.graph_objects as go
import pydeck as pdk
import logging
import data_pull_transform as dp

# --- GLOBALS
START_DATE = dp.START_DATE
END_DATE = dp.END_DATE
UNHCR_APP = dp.UNHCR_APP
SOURCE_TEXTS = "assets/text.xlsx"
SOURCE_METRICS = "assets/metrics.csv"
SOURCE_NEWS = "assets/tf_google_news.csv"

logging.basicConfig(filename='app.log', encoding='utf-8', level=logging.INFO)

def read_texts(source = SOURCE_TEXTS, sheet_name = 0):
    df = pd.read_excel(source, sheet_name=sheet_name)
    logging.info('Loaded texts')
    return df

def write_expander(df, title='summary_short_effects', title_col='title', text_col = 'text', expander_title='Explainer'):
    text = df[text_col][df[title_col]==title].iloc[0]
    with st.expander(expander_title):
        st.markdown(text)

def read_metrics(source = SOURCE_METRICS):
    df = pd.read_csv(source, encoding='utf-16')
    logging.info('Loaded metrics')
    return df

def read_news():
    df = dp.get_google_news()
    df = df[:5]
    # df = df.to_html(escape=False, render_links=True, justify='justify')
    df = df.replace('border="1"','border="0"')
    logging.info('Loaded news')
    return df

def write_news(df, id):
    st.markdown('> **' + df['Title'].iloc[id] + "** | *" + df['Media'].iloc[id] + "* | " + df['Link'].iloc[id] + ' | Published: ' + df['Date'].iloc[id])

def write_top5_news(df):
    write_news(df, 0)
    write_news(df, 1)
    write_news(df, 2)
    write_news(df, 3)
    write_news(df, 4)

def get_metric(df, title, value_col, title_col = 'Title', unit='default', digits = 0, digits_unit = 'default'):
    df = df
    value = df[value_col][df[title_col]==title].iloc[0]
    if unit == 'pct':
        value = "{:.1%}".format(value)
    elif unit == '%':
        value = "{:.1f}".format(value)
        value = f'{value}%'
    elif unit == 'k':
        value = "{:.1f}".format(value)
        value = f'{value}k'
    elif unit == 'mn':
        value = "{:.1f}".format(value)
        value = f'{value}mn'
    elif unit == 'bn':
        value = "{:.0f}".format(value)
        value = f'{value}bn'
    elif unit == 'default':
        if digits == 0:
            value = "{:.1f}".format(value)
            if digits_unit == 'default':
                value = f'{value}'
            else:
                value = f'{value}{digits_unit}'
        elif digits == 3:
            value = value / (10**3)
            value = "{:.1f}".format(value)
            if digits_unit == 'default':
                value = f'{value}k'
            else:
                value = f'{value}{digits_unit}'
        elif digits == 6:
            value = value / (10**6)
            value = "{:.1f}".format(value)
            if digits_unit == 'default':
                value = f'{value}mn'
            else:
                value = f'{value}{digits_unit}'
        elif digits == 9:
            value = value / (10**9)
            value = "{:.0f}".format(value)
            if digits_unit == 'default':
                value = f'{value}'
            else:
                value = f'{value}{digits_unit}bn'
    return value

def main():
    st.cache_resource()

    # --- LOAD DATA
    df_t = read_texts()
    df_m = read_metrics()
    df_news = read_news()

    # --- LOAD TABLES
    tab_google_news = dp.plot_google_news()

    # --- LOAD FIGURES
    fig_ccy = dp.plot_ccy_data()
    fig_refugees = dp.plot_hum_data(series = 'Refugees', title='Refugees')
    fig_idps = dp.plot_hum_data(series = 'Internally Displaced', title='Internally Displaced')
    fig_civs_dead = dp.plot_hum_data(series = 'Civilian deaths, confirmed', title='Civilian deaths, confirmed')
    fig_civs_injured = dp.plot_hum_data(series = 'Civilians injured, confirmed', title='Civilians injured, confirmed')
    fig_reconstruction_damage = dp.plot_reconstruction_sectors(series = 'Damage', title = 'Damage assessment as of February 2024, USD bn')
    fig_reconstruction_needs = dp.plot_reconstruction_sectors(series = 'Needs', title = 'Reconstruction needs assessment as of February 2024, USD bn')
    fig_reconstruction_regions = dp.plot_reconstruction_regions()
    fig_ukraine_support_committed = dp.plot_ukraine_support(series='Value committed', title = 'Support publicly announced, USD bn')
    fig_ukraine_support_delivered = dp.plot_ukraine_support(series='Value delivered', title = 'Support delivered in cash and kind, USD bn')
    # fig_grain_destinations = dp.plot_grain_destinations()
    fig_delivery_rate = dp.plot_delivery_rate()
    fig_cpi_last = dp.plot_cpi_last()
    fig_cpi_12m = dp.plot_cpi_12m()
    fig_international_reserves = dp.plot_international_reserves()
    fig_bond_yields = dp.plot_bond_yields()
    fig_policy_rates = dp.plot_policy_rate()
    fig_interest_rates = dp.plot_interest_rates()
    fig_fiscal_income = dp.plot_fiscal_income()
    fig_fiscal_expenses = dp.plot_fiscal_expenses()
    fig_fiscal_finance = dp.plot_fiscal_finance()
    fig_fsi_npl = dp.plot_financial_soundness(series='Nonperforming loans net of provisions to capital')
    fig_fsi_liquidity = dp.plot_financial_soundness(series='Net open position in foreign exchange to capital')
    # fig_fatalities_count = dp.plot_fatalities_series(series = 'FATALITIES', title = 'Number of Fatalities')
    # fig_battle_count = dp.plot_fatalities_series(series='COUNT', title = 'Number of conflict events')
    # fig_fatalities_geo = dp.plot_fatalities_geo()

    # FINAL REPORT
    st.title('Humanitarian and Economic Situation in Ukraine')
    with st.expander('Chapters'):
        st.markdown(
        """
        - [War and People](#war-and-people)
        - [War and Economics](#war-and-economics)
        - [War and Cooperation](#war-and-cooperation)
        """)
    st.header('Key indicators')
    m1, m2 = st.columns(2)
    m1.metric(
        "Refugees", 
        value = get_metric(df_m, 'Refugees', 'Last value', digits=6),
        delta = get_metric(df_m, 'Refugees', 'Change', digits=6),
        delta_color = 'inverse' 
        )
    m2.metric(
        "Internally displaced", 
        value = get_metric(df_m, 'Internally displaced', 'Last value', digits=6),
        delta = get_metric(df_m, 'Internally displaced', 'Change', digits=6),
        delta_color = 'inverse' 
        )
    m1, m2 = st.columns(2)
    m1.metric(
        "Reconstruction needs estimated, USD", 
        value = get_metric(df_m, 'Reconstruction needs', 'Last value', unit='bn'),
        )
    m2.metric(
        "UA International Reserves, USD bn", 
        value = get_metric(df_m, 'International Reserves', 'Last value', unit='bn'),
        )
    st.markdown('---')
    st.subheader('Latest news')
    st.markdown('*Google News Feed*')
    write_expander(df_t,title='Summary', expander_title='Summary')
    write_top5_news(df_news)
    st.write('')
    st.write('')
    st.subheader('Civilian casualties')
    m1, m2 = st.columns(2)
    m1.metric(
        "Civilians killed, confirmed", 
        value = get_metric(df_m, 'Civilians killed, confirmed', 'Last value', digits=3),
        delta = get_metric(df_m, 'Civilians killed, confirmed', 'Change', digits=3),
        delta_color = 'inverse' 
        )
    m2.metric(
        "Civilians injured, confirmed", 
        value = get_metric(df_m, 'Civilians injured, confirmed', 'Last value', digits=3),
        delta = get_metric(df_m, 'Civilians injured, confirmed', 'Change', digits=3),
        delta_color = 'inverse' 
        )
    write_expander(df_t,title='Casualties', expander_title='Casualties and surronding issues')
    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_civs_dead, use_container_height=400, use_container_width=300)
    col2.plotly_chart(fig_civs_injured, use_container_height=400, use_container_width=300)
    st.markdown('---')
    st.subheader('Displacement')
    m1, m2 = st.columns(2)
    m1.metric(
        "Refugees", 
        value = get_metric(df_m, 'Refugees', 'Last value', digits=6),
        delta = get_metric(df_m, 'Refugees', 'Change', digits=6),
        delta_color = 'inverse' 
        )
    m2.metric(
        "Internally displaced", 
        value = get_metric(df_m, 'Internally displaced', 'Last value', digits=6),
        delta = get_metric(df_m, 'Internally displaced', 'Change', digits=6),
        delta_color = 'inverse' 
        )
    write_expander(df_t,title='Displacement', expander_title='How to interpret displacement data')    
    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_idps, use_container_height=400, use_container_width=300)
    col2.plotly_chart(fig_refugees, use_container_height=400, use_container_width=300)
    st.subheader('Distribution of Refugees')
    st.components.v1.iframe(UNHCR_APP, width=800, height=800, scrolling=True)
    st.markdown('---')
    st.header("War and Economics")
    # Put key metrics
    st.subheader('Monetary sector')
    m1, m2, m3, m4 = st.columns(4)
    m1.metric(
        "Inflation, yoy", 
        value = get_metric(df_m, 'Inflation rate', 'Last value', unit='%'),
        delta = get_metric(df_m, 'Inflation rate', 'Change', unit='%'),
        delta_color = 'inverse' 
        )
    m2.metric(
        "Lending rate, households", 
        value = get_metric(df_m, 'Lending rate, households', 'Last value', unit='%'),
        )
    m3.metric(
        "Lending rate, corporates", 
        value = get_metric(df_m, 'Lending rate, corporates', 'Last value', unit='%'),
        )
    m4.metric(
        "FX rate: UAH/USD", 
        value = get_metric(df_m, 'FX rate: UAH/USD', 'Last value'),
        delta = get_metric(df_m, 'FX rate: UAH/USD', 'Change'),
        delta_color = 'inverse',
        )
    write_expander(df_t,title='Monetary sector', expander_title='How Ukraine adjusted monetary policy to avoid panic')
    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_cpi_12m, use_container_height=400, use_container_width=300)
    col2.plotly_chart(fig_ccy, use_container_height=400, use_container_width=300)
    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_cpi_last, use_container_height=400, use_container_width=300)
    col2.plotly_chart(fig_interest_rates, use_container_height=400, use_container_width=300)
    st.markdown('---')
    st.subheader('National bank tools')
    write_expander(df_t,title='National bank tools', expander_title='Key risks of the monetary policy')
    m1, m2 = st.columns(2)
    m1.metric(
        "Key rate", 
        value = get_metric(df_m, 'Key rate', 'Last value', unit='%'),
        )    
    m2.metric(
        "International reserves, USD", 
        value = get_metric(df_m, 'International Reserves', 'Last value', unit='bn'),
        )
    col1, col2 = st.columns(2)    
    col1.plotly_chart(fig_policy_rates, use_container_height=400, use_container_width=300)
    col2.plotly_chart(fig_international_reserves, use_container_height=400, use_container_width=300)
    st.plotly_chart(fig_bond_yields, use_container_height=400)
    st.markdown('---')
    st.subheader('Financial system')
    write_expander(df_t,title='Financial system', expander_title='How healthy are Ukranian banks?')
    m1, m2 = st.columns(2)
    m1.metric(
        "NPL ratio", 
        value = get_metric(df_m, 'NPL ratio', 'Last value', unit='%'),
        delta = get_metric(df_m, 'NPL ratio', 'Change', unit='%'),
        delta_color = 'inverse',
        )    
    m2.metric(
        "FX position to capital", 
        value = get_metric(df_m, 'FX position to capital', 'Last value', unit='%'),
        delta = get_metric(df_m, 'FX position to capital', 'Change', unit='%'),
        delta_color='normal'
        )
    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_fsi_npl, use_container_height=400, use_container_width=300)
    col2.plotly_chart(fig_fsi_liquidity, use_container_height=400, use_container_width=300)
    st.markdown('---')
    st.subheader('Government finance')
    m1, m2, m3 = st.columns(3)
    m1.metric(
        "Yield, UAH govt, bonds", 
        value = get_metric(df_m, 'Yield, UAH govt bonds', 'Last value', unit='%'),
        delta = get_metric(df_m, 'Yield, UAH govt bonds', 'Change', unit='%'),
        delta_color = 'inverse',
        )    
    m2.metric(
        "Fiscal income, UAH", 
        value = get_metric(df_m, 'Fiscal income, total', 'Last value', digits=6, digits_unit='tn'),
        )
    m3.metric(
        "Fiscal expenses, UAH", 
        value = get_metric(df_m, 'Fiscal expenses, total', 'Last value', digits=6, digits_unit='tn'),
        )
    write_expander(df_t,title='Government finance', expander_title='How much Ukraine needs to finance the war')
    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_fiscal_income, use_container_height=400, use_container_width=300)
    col2.plotly_chart(fig_fiscal_expenses, use_container_height=400, use_container_width=300)
    st.plotly_chart(fig_fiscal_finance, use_container_height=400)

#     st.header('War and cooperation')
#     st.subheader('Ukraine support')
#     write_expander(df_t,title='Ukraine support', expander_title='How much do countries follow their promise to support Ukraine')
#     m1, m2, m3 = st.columns(3)
#     m1.metric(
#         "Support announced, USD",
#         value = get_metric(df_m, 'Commitment', 'Last value', unit='bn')
#     )
#     m2.metric(
#         "Support delivered, USD",
#         value = get_metric(df_m, 'Delivered', 'Last value', unit='bn')
#     )
#     m3.metric(
#         "Military support sent, USD",
#         value = get_metric(df_m, 'Delivered military help', 'Last value', unit='bn')
#     )
# #   st.plotly_chart(fig_ukraine_support_committed, use_container_height=800, use_container_width=800)
#     st.plotly_chart(fig_ukraine_support_delivered, use_container_height=800, use_container_width=300)
#     st.plotly_chart(fig_delivery_rate)
#     st.markdown('---')
    st.header('War and reconstruction')
    # Put key metrics
    m1, m2, m3 = st.columns(3)
    m1.metric(
        "Damage estimated, USD", 
        value = get_metric(df_m, 'Damage caused', 'Last value', unit='bn'),
        )
    m2.metric(
        "Reconstruction needs estimated, USD", 
        value = get_metric(df_m, 'Reconstruction needs', 'Last value', unit='bn'),
        )
    m3.metric(
        "Ukraine GDP (2021), current USD", 
        value = get_metric(df_m, 'GDP Ukraine, current USD', 'Last value', unit='bn'),
        )
    write_expander(df_t,title='War and reconstruction', expander_title='How much Ukraine needs to rebuild the country')
    st.plotly_chart(fig_reconstruction_damage)
    st.plotly_chart(fig_reconstruction_regions, use_container_height=1600)
    st.plotly_chart(fig_reconstruction_needs)    
    st.markdown('___')
    st.header('References')
    st.markdown(
        # - ACLED. [Ukraine crisis hub](https://acleddata.com/ukraine-crisis/)
        # - Kiel Institute for the World Economy | IFW Kiel. [Ukraine support tracker](https://www.ifw-kiel.de/topics/war-against-ukraine/ukraine-support-tracker/)
        """
        - International Organisation for Migration | IOM. [Ukraine Displacement](https://displacement.iom.int/ukraine)
        - National Bank of Ukraine | NBU. [Statistics at the National Bank of Ukraine](https://bank.gov.ua/en/statistic/nbustatistic)
        - The Humanitarian Data Exchange | HDX. [Ukraine - Humanitarian Data Exchange](https://data.humdata.org/group/ukr)
        - United Nations | UN. [Black Sea Grain Initiative Joint Coordination Centre](https://www.un.org/en/black-sea-grain-initiative/)
        - UN Office of High Commissioner for Human Rights | OHCHR. [Ukraine](https://www.ohchr.org/en/countries/ukraine)
        - UN Office of High Commissioner for Refugees | UNHCR. [Ukraine Refugee Situation](https://data.unhcr.org/en/situations/ukraine)
        - World Bank (2024). [Ukraine: Rapid damage and needs assessment](https://documents1.worldbank.org/curated/en/099021324115085807/pdf/P1801741bea12c012189ca16d95d8c2556a.pdf)
        """)

if __name__ == '__main__':
    main()
