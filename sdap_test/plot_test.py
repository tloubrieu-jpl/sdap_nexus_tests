import os
import requests
from urllib.parse import quote
from datetime import datetime, timedelta
import pytz
import numpy as np
import cartopy.crs as ccrs
import matplotlib.pyplot as plt

def arr2d_from_json(js, var_name):
    return np.array([[js[i][j][var_name] for j in range(len(js[0]))] for i in range(len(js))])

def arr1d_from_json(js, var_name):
    return np.array([js[i][var_name] for i in range(len(js))])


def save_current_fig(plt, url):
    path = url.split('/')

    filedirs = path[2:-1]
    filedir = os.path.join('output', *filedirs)
    os.makedirs(filedir, exist_ok=True)

    filename = f"{quote(path[-1], safe='')}.png"

    plt.savefig(os.path.join(filedir, filename))


def algorithm_to_rest_end_points(algo):
    return [algo + 'Spark',
           # 'algorithms/' + algo
            ]

def plot_timeavg_map(target, dataset_shortname, start_datetime):
    stop_datetime = start_datetime + timedelta(days=30)

    min_lon = -140
    max_lon = -110
    min_lat = 20
    max_lat = 50

    algorithm = 'timeAvgMap'
    end_points = algorithm_to_rest_end_points(algorithm)

    for end_point in end_points:
        url = '{}/{}?ds={}&minLon={}&minLat={}&maxLon={}&maxLat={}&startTime={}&endTime={}'. \
            format(target, end_point, dataset_shortname,
                   min_lon, min_lat, max_lon, max_lat,
                   start_datetime.isoformat().replace('+00:00', 'Z'),
                   stop_datetime.isoformat().replace('+00:00', 'Z'))
        print(f'')
        response = requests.get(url)

        print(f'{url} status: {response.status_code} {response.elapsed}')

        if response.status_code == 200:
            tam_json_res = response.json()
            map = tam_json_res['data']
            vals = arr2d_from_json(map, 'mean')
            count =  arr2d_from_json(map, 'cnt')

            masked_vals = np.ma.array(vals, mask=count == 0)

            lons = arr1d_from_json(map[0], 'lon')
            lats = arr1d_from_json([map[i][0] for i in range(len(map))], 'lat')

            fig = plt.figure()
            ax = plt.axes(projection=ccrs.PlateCarree())
            extent = [min_lon, max_lon, min_lat, max_lat]
            ax.set_extent(extent)
            ax.coastlines()

            c = ax.contourf(lons, lats, masked_vals, cmap='jet')
            cbar = fig.colorbar(c)
            plt.title(url)

            save_current_fig(plt, url)

            plt.show()
        else:
            print(response.text)


def plot_time_series(target, dataset_shortname, start_datetime):
    stop_datetime = start_datetime + timedelta(days=30)

    min_lon = -140
    max_lon = -110
    min_lat = 20
    max_lat = 50

    algorithm = 'timeSeries'
    end_points = algorithm_to_rest_end_points(algorithm)

    for end_point in end_points:
        url = '{}/{}?ds={}&minLon={}&minLat={}&maxLon={}&maxLat={}&startTime={}&endTime={}'. \
            format(target, end_point, dataset_shortname,
                   min_lon, min_lat, max_lon, max_lat,
                   start_datetime.isoformat().replace('+00:00', 'Z'),
                   stop_datetime.isoformat().replace('+00:00', 'Z'))
        response = requests.get(url)

        print(f'{url} status: {response.status_code} {response.elapsed}')

        if response.status_code == 200:
            ts_json = response.json()['data']

            ts = [val[0] for val in ts_json]
            ts_time = [d["time"] for d in ts]
            ts_mean = [d["mean"] for d in ts]

            ts_datetime = [datetime.utcfromtimestamp(t) for t in ts_time]
            plt.plot(ts_datetime, ts_mean, 'b-')

            plt.title(url)

            save_current_fig(plt, url)

            plt.show()


session = requests.session()

#target = 'https://test-tools.jpl.nasa.gov/sdap/nexus'
#target = 'https://test-tools.jpl.nasa.gov/sdap2/nexus'
#target = 'https://oceanworks.jpl.nasa.gov'
target = 'http://localhost:8083'

list_response = session.get("{}/list".format(target))
list = list_response.json()

for dataset in list:

    analyse_criteria = (target,
                     dataset['shortName'],
                     datetime.fromisoformat(dataset['iso_start'][:-2] + ':' + dataset['iso_start'][-2:]))

    plot_timeavg_map(*analyse_criteria)

    plot_time_series(*analyse_criteria)







