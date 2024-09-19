import json
from datetime import date, datetime
import time
import os
import random
from glob import glob
import csv

import requests
from bs4 import BeautifulSoup


url = 'https://sdsos.gov/general-services/concealed-pistol-permits/pistolstatistics.aspx'

file_snapshots = 'snapshots.json'
file_csv = 'south-dakota-pistol-permit-stats.csv'


def get_list():
    params = {
        'url': url,
        'output': 'json'
    }

    req = requests.get(
        'https://web.archive.org/cdx/search/cdx',
        params=params
    )

    req.raise_for_status()

    data = req.json()

    data_out = {}

    for snp in data[1:]:
        timestamp = snp[1]
        snp_url = snp[2]
        snapshot_url = f'https://web.archive.org/web/{timestamp}/{snp_url}'

        data_out[timestamp] = {
            'snapshot_date': date.fromisoformat(timestamp[:8]).isoformat(),
            'snapshot_url': snapshot_url
        }

    with open(file_snapshots, 'w') as outfile:
        json.dump(
            data_out,
            outfile,
            indent=4
        )


def download_pages():
    with open(file_snapshots, 'r') as infile:
        data = json.load(infile)

    for item in data:
        filepath = f'pages/{item}.html'

        if os.path.exists(filepath):
            continue

        req = requests.get(
            data.get(item).get('snapshot_url'),
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
            }
        )
        req.raise_for_status()

        time.sleep(random.uniform(1, 3))

        with open(filepath, 'w') as outfile:
            outfile.write(req.text)

        print(f'Wrote {filepath}')


def scrape():
    pages = glob('pages/*html')

    data_out = []

    with open(file_snapshots, 'r') as infile:
        lookup = json.load(infile)

    snapshot_data = {}

    for page in pages:

        timestamp = page.split('/')[-1].split('.')[0]
        deets = lookup.get(timestamp)

        d = {
            'archive_url': deets.get('snapshot_url'),
            'archive_date': deets.get('snapshot_date')
        }

        with open(page, 'r') as infile:
            html = infile.read()

        soup = BeautifulSoup(html, 'html.parser')

        target_hed = [x for x in soup.find_all('h2') if 'total active pistol permits' in x.text.lower()][0]

        asof_date = target_hed.text.split('as of')[-1].strip()

        asof_date = datetime.strptime(
            asof_date,
            '%m/%d/%Y'
        ).date().isoformat()

        d['snapshot_date'] = asof_date

        table = target_hed.find_next_sibling('table')
        rows = table.find_all('tr')

        for row in rows:
            cat, value = [x.text.lower() for x in row.find_all('td')]
            value = int(value.replace(',', ''))
            d[cat] = value

        data_out.append(d)

    data_out.sort(
        key=lambda x: (
            x['snapshot_date'],
            x['archive_date']
        )
    )

    # in all but one case, the correct
    # numbers are in the most recent archival, so
    # the strategy is to loop over a sorted version
    # of the data and overwrite a dict with the latest
    # data, fixing one exception along the way
    keep = {}

    for archival in data_out:

        snapshot_date = archival['snapshot_date']

        # fix the one exception
        if snapshot_date == '2020-03-31':
            archival['total'] = 95795

        keep[snapshot_date] = archival

    with open(file_csv, 'w') as outfile:
        writer = csv.DictWriter(
            outfile,
            fieldnames=[
                'snapshot_date',
                'regular',
                'gold',
                'enhanced',
                'total',
                'archive_url',
                'archive_date'
            ]
        )

        writer.writeheader()
        writer.writerows(list(keep.values()))


if __name__ == '__main__':
    get_list()
    download_pages()
    scrape()
