import requests
from multiprocessing import Pool
from sdap_test.print_response import print_empty, print_all, print_list, print_data


#sdap_url = 'http://a3546b124416511ea8568067269b49ae-459062424.us-west-2.elb.amazonaws.com/nexus'
#sdap_url = 'http://podaac-devwhale1.jpl.nasa.gov:9999/test-tools/sdap2/nexus/'
#sdap_url = 'http://test-tools.jpl.nasa.gov/sdap/nexus'
sdap_url = 'http://localhost:8083/'

primary_dataset = 'mur25-jpl-l4-glob-v42-analysed-sst'
secondary_dataset = 'mur25-jpl-l4-glob-v42-analysed-sst-tiles500'
parameter = 'sst'

params = {
    'ds': primary_dataset,
    'b' : "-135, 40, -130, 50",
    'startTime': '2020-07-01T00:00:00Z',
    'endTime' :  '2020-07-29T00:00:00Z',
}



params = {
    'ds': primary_dataset,
    'minLon' : -135,
    'minLat': 40,
    'maxLon': -130,
    'maxLat': 50,
    'startTime': '2020-07-01T00:00:00Z',
    'endTime' :  '2020-07-29T00:00:00Z',
}

match_up_params = {
    'primary': primary_dataset,
    'matchup': secondary_dataset,
    'parameter': 'sst',
    'minLat': 40,
    'maxLon': -130,
    'maxLat': 50,
    'startTime': '2020-07-01T00:00:00Z',
    'endTime' :  '2020-07-29T00:00:00Z',
}

anomaly_params =  {
    'dataset': primary_dataset,
    'climatology': secondary_dataset,
    'b': '-135,40,-130,50',
    'startTime': '2020-07-01T00:00:00Z',
    'endTime' :  '2020-07-29T00:00:00Z',
}

corr_params = {
    'ds': f'{primary_dataset},{secondary_dataset}',
    'minLon' : -135,
    'minLat': 40,
    'maxLon': -130,
    'maxLat': 50,
    'startTime': '2020-07-01T00:00:00Z',
    'endTime' :  '2020-07-29T00:00:00Z',
}

operations = {
              # algorithms
              'list': {'params' : {}, 'print_callback' : print_list},
              'capabilities': {'params' : {}, 'print_callback' : print_list},
              'heartbeat': {'params': {}, 'print_callback': print_all},
              'delay' :  {'params': {}, 'print_callback': print_empty},
              'makeerror': {'params': {}, 'print_callback': print_all},
              # algorithm doms
              #'domslist': {'params' : {}, 'print_callback' : print_list},
              # algorithm spark
              'timeAvgMapSpark' : {'params' : params, 'print_callback' : print_data},
              'timeSeriesSpark': {'params': params, 'print_callback': print_data},
              'varianceSpark' : {'params' : params, 'print_callback' : print_data},
              #'match_spark' : {'params' : match_up_params, 'print_callback' : print_data},
              'latitudeTimeHofMoellerSpark': {'params': params, 'print_callback': print_data},
              'longitudeTimeHofMoellerSpark': {'params': params, 'print_callback': print_data},
              #'dailydifferenceaverage_spark': {'params': anomaly_params, 'print_callback': print_data},
              #'corrMapSpark': {'params': corr_params, 'print_callback': print_data},
              #'climMapSpark' : {'params': params, 'print_callback': print_data}
              }




def replicated_params(params, n=1):
    params_list = [params]

    for i in range(n):
        params_copy = params.copy()
        if 'minLon' in params_copy:
            params_copy['minLon'] = params_copy['minLon']+6
        if 'maxLon' in  params_copy:
            params_copy['maxLon'] = params_copy['maxLon']+6
        params_list.append(params_copy)
    return params_list


def fetch(session, url, params, callback):
    print(f"launch {url.split('/')[-1]} request with params {params}")
    with session.get(url, params=params) as response:
        print(response.url)
        if response.status_code != 200:
            print(f"FAILURE::{url}, params: {params} {response.url} {response.json()}")
        callback([params, response])

def parallel_requests(session, operation_url, param_list, callback):
    args = []
    for params in param_list:
        args.append((session, operation_url, params, callback))

    with Pool(2) as p:
        p.starmap(fetch, args)


def test_operation(session, sdap_url, op_name, op_desc):
    operation_url = f"{sdap_url.strip('/')}/{op_name.strip('/')}"
    parallel_requests(session,
                      operation_url,
                      replicated_params(op_desc['params'], n=2),
                      op_desc['print_callback'])
    print('')


def main():
    with requests.Session() as session:
        #test_operation(session, sdap_url, 'domslist', operations['domslist'])
        for operation, desc in operations.items():
            test_operation(session, sdap_url, operation, desc)


if __name__ == '__main__':
    main()







