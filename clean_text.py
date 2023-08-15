import pymongo
from pymongo.cursor import CursorType
import re

'''
cleaning up the scraped job descriptions!
I'll standardize all regex \s to single space " " and that will include \n
other cleanup:
remove CSS class info from tags
remove <div> elements
add spaces between tags for tokenization on whitespace
delete characters outside Latin, Latin Extended, some extended punctuation/currency/etc
'''

# compile regex for newlines once for the whole file
not_char = re.compile(r'[^\u0000-\u024f\u2010-\u204e\u2070-\u20cf]')
no_div = re.compile(r'</?div>')
standard_space = re.compile(r'\s+')
css_class = re.compile(r'\sclass="job.*?"')
before_tag = re.compile(r'(\S)(</?\w+?)(?=>)')
after_tag = re.compile(r'(?<=<)(/?\w+?>)(\S)')


def clean_text(text: str) -> str:
    text = text.replace("&amp;", "&")
    text = re.sub(css_class, "", text)
    text = re.sub(no_div, "", text)
    text = re.sub(not_char, "", text)
    text = re.sub(standard_space, " ", text)
    text = re.sub(before_tag, r'\1 \2', text)
    text = re.sub(after_tag, r'\1 \2', text)
    return text


# mongo aggregate pipeline to select duplicate descriptions
pipeline1 = [
    {'$group': {
        '_id': {'descr': '$descr', 'org': '$org'},
        'titles': {'$addToSet': '$occ'},
        'ids': {'$push': '$_id'},
        'count': {'$sum': 1}
    }},
    {'$match': {'count': {'$gt': 1}}},
    {'$sort': {'count': -1}}
]

if __name__ == '__main__':
    mclient = pymongo.MongoClient()
    db = mclient.job_data_db
    posts = db.august_jobs
    # remove duplicate descriptions
    repeats = list(posts.aggregate(pipeline1, allowDiskUse=True))  # group by descr, filter count > 1
    for i in range(0, 10):  # print the top 10 repeat offenders, just for fun
        print(f"Company: {repeats[i]['_id']['org']}   "
              f"Title(s): {repeats[i]['titles']}  "
              f"Repeats: {repeats[i]['count']}\n")
    print(posts.count_documents({}))  # 200603
    for each in repeats:  # remove repeats from the data!
        each['ids'] = each['ids'][1:]
        db.posts.delete_many({'_id': {'$in': each['ids']}})
    print(posts.count_documents({}))  # 197335

    # now to fix ALL the remaining docs encoding/newlines/&amp;
    dbCursor = posts.find(cursor_type=CursorType.EXHAUST)
    done_ids = dict()
    for doc in dbCursor:
        if done_ids.get(doc['_id']):
            pass
        else:
            descr = doc['descr']  # .encode('utf-8').decode('unicode_escape')
            descr = clean_text(descr)
            u = posts.update_one(filter={'_id': doc['_id']}, update={'$set': {'descr': descr}})
            done_ids[doc['_id']] = True
            with open('update_desc.txt', 'a') as f:
                f.write(f"Matches for id {doc['_id']}: {u.matched_count}, Updates made: {u.modified_count}\n")
    mclient.close()
