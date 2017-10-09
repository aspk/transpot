from datetime import datetime


def parse_header(row, column_index):
    for i in xrange(len(row)):
        header = row[i].lower()
        if "start" in header or "pickup" in header:
            if "lon" in header:
                column_index['pickup_long'] = i
            elif "lat" in header:
                column_index['pickup_lat'] = i
            elif "time" in header:
                column_index['pickup_datetime'] = i
        elif "end" in header or "dropoff" in header or "stop" in header:
            if "lon" in header:
                column_index['dropoff_long'] = i
            elif "lat" in header:
                column_index['dropoff_lat'] = i
            elif "time" in header:
                column_index['dropoff_datetime'] = i
        elif "duration" in header:
            column_index['duration'] = i
        elif "distance" in header:
            column_index['distance'] = i
        elif "total_amount" in header:
            column_index['cost'] = i
        elif "bike" in header:
            column_index['id'] = i


def convert_date(original_date):
    try:
        new_date = datetime.datetime.strptime(original_date, "%Y-%m-%d %H:%M:%S")
        return datetime.date.strftime(new_date, "%m/%d/%Y %H:%M:%S")
    except ValueError:
        return original_date
