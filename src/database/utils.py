import datetime


def convert_to_snake_case(text):
    return ''.join(['_' + i.lower() if i.isupper() else i for i in text]).lstrip('_')


def parse_floats(data, keys):
    for key in keys:
        if key in data:
            data[key] = float(data[key])

def parse_dates(data, keys, in_milliseconds=False):
    for key in keys:
        if key in data:
            timestamp = data[key]['$date']
            timestamp = timestamp / 1000 if in_milliseconds else timestamp
            data[key] = datetime.datetime.fromtimestamp(timestamp)
