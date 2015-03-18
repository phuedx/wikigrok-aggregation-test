from collections import OrderedDict
import csv
import hashlib
from time import strptime
import os

import yaml

from core import pwb, pywikibot


class Aggregator():
    def __init__(self):
        self.config = {}
        self.load_config(os.path.join(os.path.curdir, 'config.yaml'))
        self.claims = []

    def load_config(self, config_path):
        """Load config"""
        with open(config_path) as config_file:
            self.config.update(yaml.load(config_file))

    def aggregate(self):
        raw_claims = open('claims.tsv', 'r')
        claims = csv.DictReader(raw_claims, delimiter='\t')
        aggregated_claims = OrderedDict()
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
            if aggregated_claim['votes'] >= self.config['number_of_responses'] and\
                    float(aggregated_claim['positive_votes']) / aggregated_claim['votes'] >=\
                    float(self.config['percent_agreement']) / 100:
                self.claims.append(aggregated_claim)
                # print('-' * 10)
                # print('CLAIM: ' + str(aggregated_claim['claim']))
                # print('VOTES: ' + str(aggregated_claim['votes']))
                # print('POSITIVE_VOTES: ' + str(aggregated_claim['positive_votes']))

    def push(self):
        pushed_claims = 0
        if self.config['max_claims'] == -1:
            max_claims = len(self.claims)
        else:
            max_claims = self.config['offset'] + self.config['max_claims']

        site = pywikibot.Site('en', 'wikipedia')
        repo = site.data_repository()
        album_id = 'Q482994'  # album

        for claim in self.claims[self.config['offset']:max_claims]:
            subject_id = claim['claim']['event_subjectId']
            property_id = claim['claim']['event_propertyId']
            value_id = claim['claim']['event_valueId']

            item = pywikibot.ItemPage(repo, subject_id)
            item.get()

            if item.claims:
                claim_items = []
                if property_id in item.claims:
                    claim_items = [x.getTarget().title() for x in item.claims[property_id]]

                # remove 'album' if we are adding 'studio album' or 'live album'
                # 'Q208569' - studio album, 'Q209939' - live album
                if property_id == 'P31' and\
                        value_id in ('Q208569', 'Q209939') and album_id in claim_items:
                    claim_to_remove = pywikibot.Claim(
                        site, property_id, datatype='wikibase-item')
                    claim_to_remove_item = pywikibot.ItemPage(repo, album_id)
                    claim_to_remove.setTarget(claim_to_remove_item)
                    # TODO: enable the next line if you want to remove 'album' claim
                    # item.removeClaims([claim_to_remove])

                if property_id not in item.claims or value_id not in claim_items:
                    new_claim = pywikibot.Claim(
                        site, claim['claim']['event_propertyId'], datatype='wikibase-item')
                    new_claim_item = pywikibot.ItemPage(repo, claim['claim']['event_valueId'])
                    new_claim.setTarget(new_claim_item)
                    # TODO: enable the next line if you want to push data to wikidata
                    # item.addClaim(new_claim)
                    pushed_claims += 1

        print('# of qualified claims: %d' % (max_claims - self.config['offset']))
        print('# of pushed claims: %d' % pushed_claims)

if __name__ == '__main__':
    a = Aggregator()
    a.aggregate()
    a.push()

