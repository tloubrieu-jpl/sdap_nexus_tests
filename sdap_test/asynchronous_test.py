import asyncio
import requests
import time
import logging
from datetime import datetime, timedelta


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def get_async_job(url):

    response = requests.get(url)

    if response.status_code == 200:
        logger.debug(f'result from {url} ready')
        return response.json()
    elif response.status_code == 202:
        print(f'result from {url} not ready yet, wait 10s more ')
        await asyncio.sleep(10)
        return await get_async_job(url)


def algo_url_to_job_url(url):
    end_point_idx = url.find('algorithms')
    if end_point_idx > -1:
        url_base = url[:end_point_idx-1]
        new_url = f"{url_base}/jobs"
        return new_url
    else:
        logger.error(f'algorithm not found in url {url}')

async def get_async_algo(url):
    response = requests.get(url)

    if response.status_code == 200:
        logger.debug(f'result from {url} ready')
        return response.json()
    elif response.status_code == 202:
        logger.debug(f'result from {url} not ready yet, switch to nexus async mode')
        result = response.json()
        if 'job_id' in result:
            logger.debug(f"result from {url} is being processed as job {result['job_id']}")
            new_url = f"{algo_url_to_job_url(url)}/{result['job_id']}"
            return await get_async_job(new_url)
        else:
            logger.error(f'job id not found in result {result} from request url {url}')
    else:
        logger.error(f'request error from {url}, status_code {response.status_code}')


async def time_avg_map(target, dataset, start_datetime, stop_datetime, min_lon, max_lon, min_lat, max_lat):
    start_time = time.time()

    end_point = 'algorithms/timeAvgMap'
    url = '{}/{}?ds={}&minLon={}&minLat={}&maxLon={}&maxLat={}&startTime={}&endTime={}'. \
                format(target, end_point, dataset,
                       min_lon, min_lat, max_lon, max_lat,
                       start_datetime.isoformat().replace('+00:00', 'Z'),
                       stop_datetime.isoformat().replace('+00:00', 'Z'))

    result = await get_async_algo(url)
    elapsed_time = time.time() - start_time
    logger.info(f'request succeed {url} in {elapsed_time}: {result}')


async def long_request(target, dataset, start_time):
    ## long request
    min_lon = -140
    max_lon = -110
    min_lat = 20
    max_lat = 50
    stop_time = start_time + timedelta(days=90)

    await time_avg_map(target, dataset, start_time, stop_time, min_lon, max_lon, min_lat, max_lat)

async def short_request(target, dataset, start_time):
    min_lon = -140
    max_lon = -130
    min_lat = 40
    max_lat = 50
    stop_time = start_time + timedelta(days=10)

    await time_avg_map(target, dataset, start_time, stop_time, min_lon, max_lon, min_lat, max_lat)

async def main():
    target = 'https://test-tools.jpl.nasa.gov/sdap/nexus'
    dataset = "mur25-jpl-l4-glob-v42-analysed-sst"
    start_datetime = datetime.fromisoformat("2009-04-02T00:00:00+00:00")

    long_task = asyncio.create_task(long_request(target, dataset, start_datetime))
    await asyncio.sleep(20)
    short_task = asyncio.create_task(short_request(target, dataset, start_datetime))

    await long_task
    await short_task



if __name__ == '__main__':
    asyncio.run(main())

