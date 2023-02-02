import os
import csv
import glob

import requests
from bs4 import BeautifulSoup


csv_file_out = 'us-county-irrigation-estimates.csv'

csv_headers = [
    'county_fips',
    'year',
    'water_source',
    'crop',
    'cubic_km_estimate'
]

data_dir = 'data'

if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def download_csvs():
    r = requests.get('https://databank.illinois.edu/datasets/IDB-4607538')

    r.raise_for_status()
    
    soup = BeautifulSoup(r.text, 'html.parser')
    
    table = soup.find('table', {'id': 'show-files-table'})
    
    rows = [x for x in table.find_all('tr') if 'csv' in x.text]
    
    links = [f"https://databank.illinois.edu{x.find('a')['href']}" for x in rows]  # noqa

    for url in links:
        filename = f"{url.split('/')[-2]}.csv"
        
        filepath = os.path.join(
            data_dir,
            filename
        )
        
        if not os.path.exists(filepath):
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                
                with open(filepath, 'wb') as file:
                    for chunk in r.iter_content():
                        file.write(chunk)
            
            print(f'Downloaded {filepath}')


def process_data_files():
    all_data = []
    csv_files = glob.glob(f'{data_dir}/*.csv')

    for f in csv_files:
        with open(f, 'r') as infile:
            data = list(csv.reader(infile))

        headers = data.pop(0)
        
        for row in data:
            fips = row[0]

            for i, value in enumerate(row):

                if i == 0:
                    continue

                label = headers[i]

                water, crop, year = label.split('.')

                data = [
                    fips,
                    year,
                    water,
                    crop,
                    value
                ]

                all_data.append(dict(zip(csv_headers, data)))

    all_data = sorted(all_data, key=lambda k: (k['year'], k['county_fips']))

    with open(csv_file_out, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(all_data)


if __name__ == '__main__':
    download_csvs()
    process_data_files()
