import csv
import hashlib
from time import strptime
import os

import yaml


class Aggregator():
    def __init__(self):
        self.config = {}
        self.load_config(os.path.join(os.path.curdir, 'config.yaml'))

    def load_config(self, config_path):
        """Load config"""
        with open(config_path) as config_file:
            self.config.update(yaml.load(config_file))

    def aggregate(self):
        raw_claims = open('claims.tsv', 'r')
        claims = csv.DictReader(raw_claims, delimiter='\t')
        aggregated_claims = {}
        cutoff_time = strptime('2014-12-12', '%Y-%m-%d')

        for claim in claims:
            # we don't need claims before 2014-12-12
            if cutoff_time > strptime(claim['timestamp'], '%Y%m%d%H%M%S'):
                continue
            hash_ = hashlib.md5()
            hash_.update(claim['event_pageId'].encode('utf-8'))
            hash_.update(claim['event_propertyId'].encode('utf-8'))
            hash_.update(claim['event_subjectId'].encode('utf-8'))
            hash_.update(claim['event_valueId'].encode('utf-8'))
            key = hash_.hexdigest()

            if key not in aggregated_claims:
                aggregated_claims[key] = {}
                aggregated_claims[key]['claim'] = claim
                aggregated_claims[key]['votes'] = 0
                aggregated_claims[key]['positive_votes'] = 0

            aggregated_claims[key]['votes'] += 1

            if claim['event_response'] == 'NULL':
                claim['event_response'] = 0

            is_positive_vote = bool(int(claim['event_response']))

            if is_positive_vote:
                aggregated_claims[key]['positive_votes'] += 1

        for key, aggregated_claim in aggregated_claims.items():
            if aggregated_claim['votes'] >= self.config['number_of_responses']:
                print('-' * 10)
                print('CLAIM: ' + str(aggregated_claim['claim']))
                print('VOTES: ' + str(aggregated_claim['votes']))
                print('POSITIVE_VOTES: ' + str(aggregated_claim['positive_votes']))

    def push(self):
        # TODO: push data to wikidata
        pass


if __name__ == '__main__':
    ad = Aggregator()
    ad.aggregate()

