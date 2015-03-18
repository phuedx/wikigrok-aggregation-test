from collections import OrderedDict
import csv
import hashlib
from time import strptime
import os

import yaml

import pywikibot
import wikigrok.claim
import wikigrok.aggregated_claim
from pipe import *


class Aggregator():
    def __init__(self):
        self.config = {}
        self.load_config(os.path.join(os.path.curdir, 'config.yaml'))
        self.claims = []

    def load_config(self, config_path):
        """Load config"""
        with open(config_path) as config_file:
            self.config.update(yaml.load(config_file))

    def _get_claims(self):
        raw_events = open('claims.tsv', 'r')
        events = csv.DictReader(raw_events, delimiter='\t')

        for event in events:
            yield wikigrok.claim.from_event(event)

    def aggregate(self):

        # Claims submitted before 2014-12-12 were test claims.
        # FIXME: Why not filter out events where event_testing is truthy?
        start_time = strptime('2014-12-12', '%Y-%m-%d')

        grouped_claims = self._get_claims()\
               | where(lambda c: c.timestamp > start_time)\
               | groupby(lambda c: c.get_group_id())

        aggregated_claims = grouped_claims\
               | select(lambda (_, claims): wikigrok.aggregated_claim.from_claims(claims))\
               | where(lambda g: g.num_votes >= self.config['number_of_responses'])\
               | where(lambda g: g.percent_positive_votes >= self.config['percent_agreement'])

        self.claims = aggregated_claims | as_list

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
            subject_id = claim.subject_id
            property_id = claim.property_id
            value_id = claim.value_id

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
                        site, property_id, datatype='wikibase-item')
                    new_claim_item = pywikibot.ItemPage(repo, value_id)
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
