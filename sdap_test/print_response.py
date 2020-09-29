import json


def print_empty(arg):
    [params, response] = arg
    print(params)
    print(response.text)


def print_all(arg):
    [params, response] = arg
    try:
        print(params)
        print(response.json())
    except json.decoder.JSONDecodeError as e:
        print(e)


def print_list(arg):
    [params, response] = arg
    try:
        print(params)
        print(response.json()[0:3])
    except json.decoder.JSONDecodeError as e:
        print(e)


def print_data(arg):
    [params, response] = arg
    try:
        print(params)
        #print(response.text)
        print(response.json()['data'][0:3])
    except json.decoder.JSONDecodeError as e:
        print(e)
    except KeyError as e:
        print(e)