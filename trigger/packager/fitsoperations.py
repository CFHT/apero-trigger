from ..common import log

def get_card(header, keyword):
    return (keyword, header[keyword], header.comments[keyword])


def remove_keys(header, keys):
    for key in keys:
        header.remove(key, ignore_missing=True)


def verify_duplicate_cards(header, cards):
    dupe_keys = []
    for card in cards:
        key, value = card[0], card[1]  # Split up so it still works if len(card) is 3
        if key in ('SIMPLE', 'EXTEND', 'NEXTEND'):
            continue
        if header.get(key) == value:
            dupe_keys.append(key)
        else:
            extname = header.get('EXTNAME')
            log.warning('Header key %s expected to be duplicate in extension %s but was not', key, extname)
    return dupe_keys
