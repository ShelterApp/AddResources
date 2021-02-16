import sys
import os
import re

"""
This script can be used check fuzzy matching logic on a single entry.
Run this script as follows 

python fuzzy_matching.py "OREGON FOOD BANK, INC." "97211" "services"


"""

_i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _i not in sys.path:
    # add parent directory to sys.path so utils module is accessible
    sys.path.insert(0, _i)
del _i  # clean up global name space
from shared_code.utils import (
    check_similarity, locate_potential_duplicate, get_mongo_client, distance
)

if __name__ == "__main__":
    client = get_mongo_client()
    if len(sys.argv) < 3:
        print('Fewer than required arguments received. Required 3, Received ' + len(sys.argv))

    name = sys.argv[1]
    zip = sys.argv[2]
    check_collection = 'services' #collection to check ngrams against.
    if len(sys.argv) >= 4:
        check_collection = sys.argv[3]
    dc = locate_potential_duplicate(name, zip, client, check_collection)

    print('Possible match from `' + check_collection + '` collection is: ')
    print(dc)

    if dc is not False:
        new_service = name
        existing_service = dc
        regex = r'(st\.? |saint | inc\.?| nfp)'
        new_subbed_service = re.sub(regex, '', new_service).lower()
        existing_subbed_service = re.sub(regex, '', existing_service).lower()

        edit_distance = distance(new_subbed_service, existing_subbed_service)
        print("edit distance: " + str(edit_distance))
        if (edit_distance >= 0.9):
            print("Will be added as a duplicate.")
        else:
            print("Not a duplicate")
    else: 
        print("Not a duplicate")