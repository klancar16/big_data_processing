import sys
import requests
import json
import time

if __name__ == '__main__':
    output_file = sys.argv[1]
    interval = int(sys.argv[2])
    full_time = int(sys.argv[3])

    journey_url = 'http://data.itsfactory.fi/journeys/api/1/vehicle-activity'
    data = []

    current_time = 0
    while current_time < full_time:
        # print('{0}/{1}'.format(current_time, full_time))
        journey_json = requests.get(journey_url).json()
        data.append(journey_json)
        time.sleep(interval)
        current_time = current_time + interval

    with open(output_file, 'w') as file:
        json.dump(data, file)
