from calendar import c
import requests
import etl

host_args = {
						'host' : 'localhost',
						'user' : 'golden',
						'password' : 'password',
						'database' : 'air_pollution'
						}
etl = etl.ETL(**host_args)

#website='https://rapidapi.com/apidojo/api/airvisual1/'
url = "https://airvisual1.p.rapidapi.com/v2/auto-complete"
cities_id = []
airpollution_data = {'data' : []}

querystring = {"q":"israel",
                "x-user-lang":"en-US",
                "x-user-timezone":"Asia/Israel",
                "x-aqi-index":"us",
                "x-units-pressure":"mbar",
                "x-units-distance":"kilometer",
                "x-units-temperature":"celsius"}

headers = {
    'x-rapidapi-host': "airvisual1.p.rapidapi.com",
    'x-rapidapi-key': "072ff69ccemshaf0b0e7d635e7b6p1dad35jsn563839ff8e2c"
    }

response = requests.request("GET", url, headers=headers, params=querystring)
cities = response.json()

if cities['status'] == 'success':
    cities_data = cities['data']['cities']
    cities_id = {city['id'] : city['city'] 
                        for city in cities_data 
                        #if city['city']=='Tel-Aviv'
                            }

for id in cities_id:
    #print(cities_id[id])        
    querystring = {"id": id,
                    "x-user-lang":"en-US",
                    "x-user-timezone":"Asia/Israel",
                    "x-aqi-index":"us",
                    "x-units-pressure":"mbar",
                    "x-units-distance":"kilometer",
                    "x-units-temperature":"celsius"}

    url = "https://airvisual1.p.rapidapi.com/cities/v2/get-measurements"
    response = requests.request("GET", url, headers=headers, params=querystring)

    city_measures = response.json()

    if city_measures['status'] == 'success':
        measures = city_measures['data']['dailyMeasurements']
        for measure in measures:
            for m in measure['measurements']:
                #print(m.values())
                measure_type=list(m.values())[1]
                measure_value=list(m.values())[0]
                measure_color=list(m.values())[2]
                measure_status=list(m.values())[3]
                airpollution_data['data'].append([measure['ts'],
                                                        cities_id[id],
                                                            measure_type,
                                                                measure_value,
                                                                    measure_color,
                                                                        measure_status])
                    
init_sql='sql/init.sql'
etl.exec_script(init_sql)

print('--------> Importing data from api ')
etl.insert_bulk('air_pollution_api', airpollution_data["data"], 'y')

air_pollution_mrr_sql='sql/air_pollution_mrr.sql'
etl.exec_script(air_pollution_mrr_sql)