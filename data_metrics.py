import pandas as pd
import logging
import data_pull_transform as dp
from datetime import datetime

# --- GLOBALS
START_DATE = dp.START_DATE
END_DATE = dp.END_DATE
UNHCR_APP = dp.UNHCR_APP
NOW = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
SOURCE_TEXTS = "assets/text.xlsx"
SOURCE_FILE = "assets/data_sources.xlsx"
SOURCE_SHEET = "assets"
SOURCE_FOLDER = SOURCE_SHEET
EXPORT_FILE = f'{SOURCE_FOLDER}/metrics.csv'

# LOGGING
logging.basicConfig(filename='data_metrics.log', encoding='utf-8', level=logging.INFO)

def get_source_files(source = SOURCE_FILE, sheet = SOURCE_SHEET):
    df = pd.read_excel(source, sheet_name=sheet)
    df = (df
          [df['Active'] == 1]
          [df['Transformed data'] == 1]
          )
    df = df[['Name', 'Shape Horisontal', 'Title', 'Subtitle', 'Value column', 'Unit', 'Condition field', 'Condition', 'Source', 'Source link']]
    df = df.reset_index()
    return df

def update_metrics():
    df = get_source_files()
    df_output = pd.DataFrame()
    for i, j in df.iterrows():
        file_name = df['Name'].iloc[i]
        link = SOURCE_FOLDER + '/' + df['Name'][df['Name']==file_name].iloc[0]
        title = df['Title'].iloc[i]
        subtitle = df['Subtitle'].iloc[i]
        source = df['Source'].iloc[i]
        source_link = df['Source link'].iloc[i]
        condition_field = df['Condition field'].iloc[i]
        condition = df['Condition'].iloc[i]
        unit = df['Unit'].iloc[i]
        value_column = df['Value column'].iloc[i]
        horisontal = df['Shape Horisontal'].iloc[i]
        # Define values
        df_temp = pd.read_csv(link, encoding='utf-16')
        if horisontal == 0:
            last_value = df_temp[value_column].iloc[-1]
            previous_value = df_temp[value_column].iloc[-2]
            change = 'NA'
            if type(last_value) == int or float:
                change = last_value - previous_value
            else:
                previous_value == 'NA'
        else:
            previous_value = 'NA'
            change = 'NA'
            if pd.isna(condition_field) == False and condition_field != '' and condition_field.lower() != 'nan':
                df_temp = df_temp[value_column][df_temp[condition_field]==condition]
                df_temp = df_temp.reset_index()
            last_value = df_temp[value_column].sum()
        rounding_list = [last_value, previous_value, change]
        for v in rounding_list:
            if type(v) == float:
                v = round(last_value, 2)
        input_dict = {
            'Title': title,
            'Subtitle': subtitle,
            'Last value': last_value,
            'Previous value': previous_value,
            'Change': change,
            'Unit': unit,
            'Source': source,
            'Source link': source_link
        }
        df_input = pd.DataFrame([input_dict])
        df_input['Last updated'] = NOW
        df_output = pd.concat([df_output, df_input], ignore_index=True)
    df_output.to_csv(EXPORT_FILE, encoding='utf-16', index=False)

if __name__ == '__main__':
    update_metrics()
