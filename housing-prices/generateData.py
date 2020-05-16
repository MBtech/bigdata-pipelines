import numpy as np
import pandas as pd 
from joblib import Parallel, delayed
import math
import sys 
import boto3
import json
import math

house_size = range(50, 1000, 10)
house_loc = range(0, 10000)
house_rooms =range(1, 12)
house_bathrooms = range(1, 12)
house_years = range(0, 100)

np.random.seed(10)
crime_ratings = {loc: np.random.choice(range(1, 11)) for loc in house_loc}
salary = {loc: np.random.choice(range(20, 100, 10)) for loc in house_loc}

def house_data(start, stop, loc_data = None, partitions=4):
    if loc_data is None:
        for p in range(0, partitions):
            end = start + (p*math.ceil((stop-start)/partitions))
            size_arr = np.random.choice(house_size, end-start)
            loc_arr = np.random.choice(house_loc, end-start)
            rooms_arr = np.random.choice(house_rooms, end-start)
            bathrooms_arr = np.random.choice(house_bathrooms, end-start)
            year_arr = np.random.choice(house_years, end-start)
            
            salary_arr =5000 * np.array([salary[k] for k in loc_arr])
            crime_ratings_arr = 5000 * np.array([crime_ratings[k] for k in loc_arr])
            price_arr = (1000*size_arr) + salary_arr + crime_ratings_arr + (1000*rooms_arr) + (500*bathrooms_arr) +  ((house_years[-1] - year_arr) *10000)
        
            indices = [i for i in range(start, end)]

            data = {"id": indices, "size": size_arr, "loc": loc_arr, "rooms": rooms_arr, "bathrooms": bathrooms_arr, 
                    "year": year_arr, "price": price_arr}
            data = pd.DataFrame(data=data)
            
            data.index.name = 'index'
            data.to_csv('sample_data/housing_data_' +str(end)+'.csv')
            s3 = boto3.resource('s3')
        
            s3.meta.client.upload_file('sample_data/housing_data_' +str(start)+'.csv', configs["bucket.name"], 'current-data/housing_data_' +str(start)+'.csv')
        return data
    else:
        for p in range(0, partitions):
            end = start + (p*math.ceil((stop-start)/partitions))    
            size_arr = np.random.choice(house_size, end-start)
            loc_arr = np.random.choice(house_loc, end-start)
            rooms_arr = np.random.choice(house_rooms, end-start)
            bathrooms_arr = np.random.choice(house_bathrooms, end-start)
            year_arr = np.random.choice(house_years, end-start)
            
            salary_arr =5000 * np.array([salary[k] for k in loc_arr])
            crime_ratings_arr = 5000 * np.array([crime_ratings[k] for k in loc_arr])
            price_arr = (1000*size_arr) + salary_arr + crime_ratings_arr + (1000*rooms_arr) + (500*bathrooms_arr) +  ((house_years[-1] - year_arr) *10000)
        
            crime_arr = np.array([loc_data.iloc[loc]["crime"] for loc in loc_arr])
            salary_arr = np.array([loc_data.iloc[loc]["salary"] for loc in loc_arr])
            indices = [i for i in range(start, end)]

            data = {"id": indices, "size": size_arr, "loc": loc_arr, "rooms": rooms_arr, "bathrooms": bathrooms_arr, 
                    "year": year_arr, "price": price_arr, "crime": crime_arr, "salary": salary_arr}
            data = pd.DataFrame(data=data)

            data.index.name = 'index'
            data.to_csv('sample_data/data_' +str(end)+'.csv')
            s3 = boto3.resource('s3')
        
            s3.meta.client.upload_file('sample_data/data_' +str(start)+'.csv', configs["bucket.name"], 'all-data/data.csv/data_' +str(start)+'.csv')

            return data

def loc_data_generator():
    data = pd.DataFrame(columns=["loc", "crime", "salary"])
    print(house_loc)
    for loc in house_loc:
        crime = crime_ratings[loc]
        s = salary[loc]
        data.loc[len(data)] = [loc, crime, s]

    data.index.name = 'index'
    data.to_csv('sample_data/loc_data.csv')
    s3 = boto3.resource('s3')
    
    s3.meta.client.upload_file('sample_data/loc_data.csv', configs["bucket.name"], 'loc_data.csv')
    return data

def init_data_generator(start=0, n_points=100000, n_jobs=4, partitions=32):
    print("Generating Init Data")
    loc_data = loc_data_generator()
    
    step = int(math.ceil(n_points/n_jobs))

    Parallel(n_jobs=n_jobs)(delayed(house_data)(i, i+step, loc_data, partitions=int(partitions/n_jobs)) for i in range(start, n_points, step))

def housing_data_generator(start=0, n_points=1000000, n_jobs=4, partitions=32):

    step = int(math.ceil(n_points/n_jobs))

    Parallel(n_jobs=n_jobs)(delayed(house_data)(i, i+step, partitions=int(partitions/n_jobs)) for i in range(start, n_points, step))



configs = json.load(open('config.json'))
partitions = 256
if sys.argv[1] == "init":
    init_data_generator(n_points=100000, n_jobs=8, partitions=partitions)
else:
    housing_data_generator(start=0, n_points=10000000, n_jobs=8, partitions=partitions)
# loc_data_generator()
