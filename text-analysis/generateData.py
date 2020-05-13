import numpy as np
import pandas as pd 
from joblib import Parallel, delayed
import math

house_size = range(50, 1000, 10)
house_loc = range(1, 10000)
house_rooms =range(1, 12)
house_bathrooms = range(1, 12)
house_years = range(0, 100)

np.random.seed(10)
crime_ratings = {loc: np.random.choice(range(1, 11)) for loc in house_loc}
salary = {loc: np.random.choice(range(20, 100, 10)) for loc in house_loc}

def house_data(start, stop):
    data = pd.DataFrame(columns=["id", "size", "loc", "rooms", "bathrooms", "year", "price"])
    for i in range(start, stop):
        size = np.random.choice(house_size)
        loc = np.random.choice(house_loc)
        rooms = np.random.choice(house_rooms)
        bathrooms = np.random.choice(house_bathrooms)
        year = np.random.choice(house_years) 
        price = (1000*size) + (5000*salary[loc]) + (5000*crime_ratings[loc]) + (1000*rooms) + (500*bathrooms) +  (house_years[-1] - year) *10000
        data.loc[len(data)] = [i, size, loc, rooms, bathrooms, year, price]
    
    data.index.name = 'index'
    data.to_csv('housing_data_' +str(start)+'.csv')

def loc_data_generator():
    data = pd.DataFrame(columns=["loc", "crime", "salary"])
    for loc in house_loc:
        crime = crime_ratings[loc]
        s = salary[loc]
        data.loc[len(data)] = [loc, crime, s]

    data.index.name = 'index'
    data.to_csv('loc_data.csv')

def housing_data_generator():
    
    n_points = 1000000
    n_jobs = 4
    step = int(math.ceil(n_points/n_jobs))

    Parallel(n_jobs=n_jobs)(delayed(house_data)(i, i+step) for i in range(0, n_points, step))


housing_data_generator()
# loc_data_generator()