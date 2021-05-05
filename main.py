from sqlalchemy import (Table, Column, String, Integer, Boolean, Date, MetaData)
import sqlalchemy
import psycopg2
import requests
import pandas as pd
from datetime import datetime
import datetime
import dateutil
import os
from dateutil.relativedelta import relativedelta
import time
import json
import itertools


KEY = 'IadmBfkxQlEaKbKJeBzGcrLEnkp815iG'
SECRET = 'BuuExFa9w5Fm6LOk'

# Date range
end = datetime.date.today()
start = end - relativedelta(years=1, months=4, days=15)

# Get months that fall in this range
months_in_range = [x.split(' ') for x in pd.date_range(start=start, end=end, freq='MS').strftime('%Y %#m').tolist()]

all_headlines = []
all_pub_date = []
all_type_material = []
all_news_desk = []

for date in months_in_range:
    year = date[0]
    month = date[1]

    r = requests.get('https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={my_key}'.format(my_key=KEY, year=year,month=month))

    data = r.json()

    top_level = data['response']['docs']

    headlines = [article['headline']['main'] for article in top_level]
    pub_date = [article['pub_date'] for article in top_level]
    type_material = [article['type_of_material'] for article in top_level]
    news_desk = [article['news_desk'] for article in top_level]

    all_headlines.append(headlines)
    all_pub_date.append(pub_date)
    all_type_material.append(type_material)
    all_news_desk.append(news_desk)

    time.sleep(6)

all_headlines = list(itertools.chain.from_iterable(all_headlines))
all_pub_date = list(itertools.chain.from_iterable(all_pub_date))
all_type_material = list(itertools.chain.from_iterable((all_type_material)))
all_news_desk = list(itertools.chain.from_iterable(all_news_desk))


nyarticles_dict = {
    'headlines': all_headlines,
    'publish_date': all_pub_date,
    'type_of_material': all_type_material,
    'news_desk': all_news_desk
}

ny_times_df = pd.DataFrame(nyarticles_dict)

ny_times_df['index'] = range(1, len(ny_times_df) + 1)

print(ny_times_df)



database = ''
username = ''
password = ''
host = ''
port = ''

try:
    engine = sqlalchemy.create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(username, password, host, port, database),
                                      echo=True,
                                      connect_args={"check_same_thread": False},
                                      poolclass=StaticPool)
    connection = engine.connect()
    print('Im entering the mainframe')

except:
    print("Your credentials are fucked up asshole")

metadata = MetaData()
nytimes = Table('ny_times', metadata,
                Column('headlines', String(255)),
                Column('publish_date', Date()),
                Column('type_material', String(255)),
                Column('news_desk', String(255)),
                Column('index', String(255), primary_key=True, unique=True))

metadata.create_all(engine)

ny_times_df.to_sql('ny_times', con=connection, if_exists='replace')