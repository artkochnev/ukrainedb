import requests as re
import logging

def ping_db(link = 'https://artkochnev-ukraine-dashboard-app-ujga5a.streamlit.app/'):
    try:
        response = re.get(link)
        print(response)
    except Exception as e:
        logging.warning(f'Could not ping the dashabord website. Exception {e}')

if __name__ == '__main__':
    ping_db()