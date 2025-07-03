import streamlit as st
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, inspect

# Reading data from csv
traffic_df = pd.read_csv('/home/ramesh/Documents/My Datasets/traffic_stops.csv')

# Cleaning Data
traffic_df.dropna(axis=1, how='all', inplace=True)
traffic_df.fillna({
    'driver_age': traffic_df['driver_age'].median(),
    'driver_gender': 'Unknown',
    'driver_race': 'Unknown',
    'violation': 'Unknown',
    'stop_outcome': 'Unknown',
    'search_type': 'Unknown'
}, inplace=True)

# Database Connection
user = "nirmal"
password = "cO4uzqqH6McrwRJlXQl1TVTHHd8is4yK"
host = "dpg-d1gngkngi27c73bul7fg-a.singapore-postgres.render.com"
database = "data_db_bsh4"

db_url=f"postgresql://{user}:{password}@{host}/{database}"

# Insert data to database
def fetch_data(query):
  engine = create_engine(db_url)
  with engine.connect() as conn:
    return pd.read_sql(query, conn)
  traffic_df.to_sql('traffic_data', engine, index=False, if_exists='replace')

# Insights
search_options = st.selectbox("Was search conducted?", options=[True, False])

search_types = traffic_df['search_type'].dropna().unique().tolist()
selected_search = st.selectbox("Select Search Type", search_types)

drug_options = st.selectbox("Was drug related?", options=[True, False])

duration = traffic_df['stop_duration'].dropna().unique().tolist()
selected_duration = st.selectbox("Duration", duration)

vehicle = traffic_df['vehicle_number'].dropna().unique().tolist()
selected_vehicle = st.selectbox("Vehicle Number", vehicle)

search_query = f"""SELECT * FROM traffic_data WHERE search_conducted IS {search_options} AND search_type = '{selected_search}' AND drugs_related_stop IS {drug_options} AND stop_duration = '{selected_duration}' AND vehicle_number = '{selected_vehicle}' """

if st.button("Search"):
  result = fetch_data(search_query)

  if not result.empty:
    row = result.iloc[0]
    st.write(f"Violation: {row['violation']}")
    st.write(f"Vehicle Number: {row['vehicle_number']}")
    st.write(f"Country: {row['country_name']}")

    st.write(f"""🚗 A {row['driver_age']}-year-old {row['driver_gender']} driver was stopped for {row['violation']} at {row['stop_time']}. A search {'was' if row['search_conducted'] else 'was not'} conducted, and they received a citation. The stop lasted {row['stop_duration']} minutes and {'was' if row['drugs_related_stop'] else 'was not'} drug-related.""")
  
  else:
    st.warning("No results found.")

# Medium Queries
st.header('Medium Queries')

medium_query_map = {
  "What are the top 10 vehicle_Number involved in drug-related stops?": "select vehicle_number, count(*) as stop_count from traffic_data where drugs_related_stop = true group by vehicle_number order by stop_count limit 10;",
  "Which vehicles were most frequently searched?": "select vehicle_number, COUNT(*) as search_count from traffic_data where search_conducted = true group by vehicle_number order by search_count desc limit 5;",
  "Which driver age group had the highest arrest rate?": "select driver_age, count(*) as search_count from traffic_data where is_arrested = true group by driver_age order by search_count limit 5;",
  "What is the gender distribution of drivers stopped in each country?": "select country_name, driver_gender from traffic_data group by driver_gender, country_name order by driver_gender, country_name;",
  "Which race and gender combination has the highest search rate?": "select driver_race, driver_gender, count(*) as search_count from traffic_data where search_conducted = true group by driver_race, driver_gender order by driver_race, driver_gender limit 5;",
  "What time of day sees the most traffic stops?": "select extract(hour from stop_time::time) as hour, count(*) as stop_count from traffic_data group by hour order by stop_count desc limit 5;",
  "Which violations are most associated with searches or arrests?": "select violation, count(*) as violation_count from traffic_data where search_conducted is true or is_arrested is true group by violation order by violation_count desc",
  "Which violations are most common among younger drivers (<25)?": "select violation, driver_age, count(*) as violation_count from traffic_data where driver_age < 25 group by violation, driver_age order by violation_count desc limit 1",
  "Which countries report the highest rate of drug-related stops?": "select country_name, violation, count(*) as stop_count from traffic_data where drugs_related_stop is true group by country_name, violation order by stop_count desc limit 1",
  "What is the arrest rate by country and violation?": "select country_name, violation, count(*) as arrest_count from traffic_data where is_arrested is true group by country_name, violation order by arrest_count desc;",
  "Which country has the most stops with search conducted?": "select country_name, count(*) as search_count from traffic_data where search_conducted is true group by country_name limit 1"
}
  
selected_query = st.selectbox("Select a Medium Level Query", list(medium_query_map.keys()))

if st.button("Run medium level query"):
    result = fetch_data(medium_query_map[selected_query])
    if not result.empty:
        st.write(result)
    else:
        st.warning("No results found")
