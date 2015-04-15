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

    def _choose_claim_for_subject(self, claims):
        sorted_claims = claims | sort(key=lambda c: c.num_votes) | as_list
        num_claims = sorted_claims | count
        candidate_claim = sorted_claims | first

        # We're only ever testing the claim with the highest number of votes.

        if num_claims == 1:
            return candidate_claim

        total_votes = sorted_claims | select(lambda c: c.num_votes) | add
        percentage_votes = float(candidate_claim.num_votes) / total_votes * 100

        if percentage_votes >= self.config['percent_agreement']:
            return candidate_claim

        return None

    def aggregate(self):

        # Claims submitted before 2014-12-12 were test claims.
        # FIXME: Why not filter out events where event_testing is truthy?
        start_time = strptime('2014-12-12', '%Y-%m-%d')

        aggregated_claims = self._get_claims()\
               | where(lambda c: c.timestamp > start_time)\
               | groupby(lambda c: c.get_group_id())\
               | select(lambda (_, claims): wikigrok.aggregated_claim.from_claims(claims))\
               | where(lambda c: c.num_votes >= self.config['number_of_responses'])

        self.claims = aggregated_claims\
               | groupby(lambda c: c.subject_id)\
               | select(lambda (_, claims): self._choose_claim_for_subject(claims))\
               | where(lambda c: c is not None)\
               | as_list

    def push(self):
        pushed_claims = 0
        if self.config['max_claims'] == -1:
            max_claims = len(self.claims)
        else:
            max_claims = self.config['offset'] + self.config['max_claims']

        site = pywikibot.Site('en', 'wikipedia')
        repo = site.data_repository()
        album_id = 'Q482994'  # album

        wikidata = pywikibot.ItemPage(repo, 'Q19801256')
        imported_from = pywikibot.Claim(repo, 'P143')
        imported_from.setTarget(wikidata)

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
                    new_claim.addSource(imported_from)
                    # TODO: enable the next line if you want to push data to wikidata
                    # item.addClaim(new_claim)
                    pushed_claims += 1

        print('# of qualified claims: %d' % (max_claims - self.config['offset']))
        print('# of pushed claims: %d' % pushed_claims)

if __name__ == '__main__':
    a = Aggregator()
    a.aggregate()
    a.push()
