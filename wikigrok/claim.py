import hashlib
import time


class Claim:
    def __init__(self, subject_id, property_id, value_id, is_positive,
                 timestamp):
        self.subject_id = subject_id
        self.property_id = property_id
        self.value_id = value_id
        self.is_positive = is_positive
        self.timestamp = timestamp
        self._group_id = None

    def get_group_id(self):
        if self._group_id is None:
            hash_ = hashlib.md5()
            hash_.update(self.property_id.encode('utf-8'))
            hash_.update(self.subject_id.encode('utf-8'))
            hash_.update(self.value_id.encode('utf-8'))

            self._group_id = hash_.hexdigest()

        return self._group_id


def from_event(event):
    event_response = event['event_response']
    is_positive = True

    if event_response == 'NULL' or int(event_response) == 0:
        is_positive = False

    return Claim(
        event['event_subjectId'],
        event['event_propertyId'],
        event['event_valueId'],
        is_positive,
        time.strptime(event['timestamp'], '%Y%m%d%H%M%S')
    )
