# checking basic info about the books dataset from Amazon
import json
from tqdm import tqdm
import gzip
import pprint



''' 
Exploratory code for book metadata
'''
# file = 'meta_Books.jsonl'
# with open(file, 'r') as f:
#     isbns = set()
#     count = 0
#     for line in tqdm(f):
#         book = json.loads(line)
#         count += 1
#         if count == 2500:
#             print(json.dumps(book, indent=4))
#             break
#         # isbn = book['details'].get('ISBN 10')
#         # isbns.add(isbn)

# print(len(isbns))


'''
Exploratory code for Open Library
'''
# f = gzip.open('data/openlibrary/ol_cdump_2025-06-30/ol_cdump_2025-06-30.txt.gz', 'rb')
# with gzip.open('data/openlibrary/ol_cdump_2025-06-30/ol_cdump_2025-06-30.txt.gz', 'rb') as f:
#     count = 0
#     for line in tqdm(f):
#         book_line = line.decode('utf-8').split('\t')
#         count += 1
#         if book_line[0] == '/type/edition' and count > 100000:
#             pprint.pp(book_line)
#             break

#         count += 1


'''
Exploratory code for books

30m ratings
4.5m books
'''
file = 'meta_Books.jsonl'
with open(file, 'r') as f:
    isbns = set()
    count = 0
    # print(len(tqdm(f)))
    for line in tqdm(f):
        book = json.loads(line)
        count += 1
        if count == 250000:
            print(json.dumps(book, indent=4))
            break
