import csv
import hashlib

raw_claims = open('claims.tsv', 'r')
claims = csv.DictReader(raw_claims, delimiter='\t')
aggregated_claims = {}

for claim in claims:
    hash = hashlib.md5()
    hash.update(claim['event_pageId'].encode('utf-8'))
    hash.update(claim['event_propertyId'].encode('utf-8'))
    hash.update(claim['event_subjectId'].encode('utf-8'))
    hash.update(claim['event_valueId'].encode('utf-8'))
    key = hash.hexdigest()

    if key not in aggregated_claims:
        aggregated_claims[key] = {}
        aggregated_claims[key]['claim'] = claim
        aggregated_claims[key]['votes'] = 0
        aggregated_claims[key]['positive_votes'] = 0;

    aggregated_claims[key]['votes'] += 1

    if (claim['event_response'] == 'NULL'):
        claim['event_response'] = 0

    is_positive_vote = bool(int(claim['event_response']))

    if is_positive_vote:
        aggregated_claims[key]['positive_votes'] += 1

for key, aggregated_claim in aggregated_claims.items():
    if aggregated_claim['votes'] >= 10:
        print('-' * 10)
        print('CLAIM: ' + str(aggregated_claim['claim']))
        print('VOTES: ' + str(aggregated_claim['votes']))
        print('POSITIVE_VOTES: ' + str(aggregated_claim['positive_votes']))
