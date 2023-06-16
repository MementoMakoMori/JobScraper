import pymongo
from pymongo.cursor import CursorType
import re

no_nl = re.compile(r'\n+')

# previously I was writing text to file encoded in utf-8 so that the newlines in data would not create newlines in file
# but forgot to remove that encoding when I switched to mongo instead of .txt files
# so now I have to undo it and remove \n for the model inputs!

# iterate over each document in posts and update the descr field to remove newlines and "&amp"

if __name__ == '__main__':
    done_ids = dict()
    mclient = pymongo.MongoClient()
    db = mclient.job_data_db
    posts = db.posts
    dbCursor = posts.find(cursor_type=CursorType.EXHAUST)
    for doc in dbCursor:
        if done_ids.get(doc['_id']):
            pass
        else:
            descr = doc['descr'].encode('utf-8').decode('unicode_escape')
            descr = descr.replace("&amp;", "&")
            descr = re.sub(no_nl, "", descr)
            u = posts.update_one(filter={'_id': doc['_id']}, update={'$set': {'descr': descr}})
            done_ids[doc['_id']] = True
            with open('update_desc.txt', 'a') as f:
                f.write(f"Matches for id {doc['_id']}: {u.matched_count}, Updates made: {u.modified_count}\n")
