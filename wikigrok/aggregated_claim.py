from pipe import *


class AggregatedClaim:
    def __init__(self, page_id, subject_id, property_id, value_id, num_votes):
        self.page_id = page_id
        self.subject_id = subject_id
        self.property_id = property_id
        self.value_id = value_id
        self.num_votes = num_votes


def from_claims(claims):

    # FIXME: Why does claims | count always yield 0?
    claims_list = claims | as_list

    claim = claims_list | first
    num_votes = claims_list | where(lambda c: c.is_positive) | count

    return AggregatedClaim(
        claim.page_id,
        claim.subject_id,
        claim.property_id,
        claim.value_id,
        num_votes
    )
