import json
import time
import pymongo
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from multiprocessing import get_context
import sys
import re
from datetime import datetime

'''
Q: Why are there so many print statements, and sys.stdout is assigned to a file?
A: logging multiprocessesing is a PITA.
the multiprocessing_logging package only works with forks, but pymongo connection is not is not fork safe 
(thus I use 'spawn' for the context)
'''

sys.stdout = open("scrape_to_db" + datetime.now().strftime("%m%d") + ".out", "a")
rm = re.compile(r'\n+')

def scrape_descr(job_ids: list):
    with sync_playwright() as p:
        browser = p.firefox.launch()
        cont = browser.new_context(record_har_content="omit")
        page = cont.new_page()
        total = []
        chunkn = job_ids[1]
        job_ids = job_ids[0]

        def block_requests(rt):  # if uBlock Origin says I don't need these, then I don't think the scraper does either
            block = {'t.indeed.com': True, 'secure.indeed.com': True, 'static.cloudflareinsights.com': True,
                     'pt.ispot.tv': True}
            if block.get(rt.request.headers['host']):
                rt.abort()
            else:
                rt.continue_()

        def go_to(url):
            nonlocal page
            while True:
                try:
                    page.goto(url, timeout=30000)
                except PlaywrightTimeoutError:
                    print(f"Chunk {chunkn}: Timeout error to {url}, attempting reload...", flush=True)
                    continue
                break

        def clean_text(text):
            text = text.replace("&amp;", "&")
            return re.sub(rm, "", text)

        def scrape(job_id):
            nonlocal page
            go_to(f'https://www.indeed.com/viewjob?jk={job_id}')
            page.wait_for_load_state("load")
            t1 = time.time()
            while "Just a moment" in page.title():  # sometimes navigation takes a while and I get this middle page
                time.sleep(0.1)
                t2 = time.time()
                if t2 - t1 >= 29:
                    print(f"Chunk {chunkn}: Load timeout on {job_id}, skipping.", flush=True)
                    return None
            if "Error" in page.title():  # received this a bunch on Jun 13 '23 when AWS US-East 1 crashed
                print(f"Chunk {chunkn}: Error accessing https://www.indeed.com/viewjob?jk={job_id}. Retrying go-to.")
                go_to(f'https://www.indeed.com/viewjob?jk={job_id}')
            elif "Page Not Found" in page.title():  # congrats to the new hires and thus job posts taken down
                print(f"Chunk {chunkn}: Job {job_id} no longer exists.", flush=True)
                return None
            occ = page.query_selector('.jobsearch-JobInfoHeader-title-container')
            while not occ:
                time.sleep(1)
                occ = page.query_selector('.jobsearch-JobInfoHeader-title-container')
                if not occ:
                    print(f"Chunk {chunkn}: Error finding job info at "
                          f"https://www.indeed.com/viewjob?jk={job_id}.\nPage dump:\n{page.title()}\n"
                          f"{page.query_selector('//body', strict=False).inner_text()}", flush=True)
                    return None
            occ = occ.inner_text()
            org = page.query_selector('//div[@id="viewJobSSRRoot"]//div[@data-company-name]', strict=False)
            org = org.inner_text()
            loc = page.query_selector('div.css-6z8o9s', strict=False)
            loc = loc.inner_text()
            text = page.query_selector('//div[@id="jobDescriptionText"]')
            body = text.inner_html()
            body = clean_text(body)
            return {"_id": job_id, "occ": occ, "org": org, "loc": loc, "descr": body}

        page.route("", block_requests)
        for num in range(0, len(job_ids)):
            res = scrape(job_ids[num])
            if res:
                total.append(res)
            if num % 20 == 0 and num != 0:
                cont.close()
                browser.close()
                browser = p.firefox.launch()
                cont = browser.new_context(record_har_content="omit")
                page = cont.new_page()
                page.route("", block_requests)
        cont.close()
        browser.close()
    print(f"Sending chunk {chunkn} to results.", flush=True)
    return total


if __name__ == '__main__':
    with open('job_ids.json', 'r') as f:
        ids = json.load(f)
        ids = list(ids.keys())
    f.close()
    chunks = list(range(0, len(ids), 500))  # split ids into chunks to send to each process
    i = 0
    id_inds = [[x] for x in range(0, len(chunks))]  # assign each chunk a number for logging
    while i < len(chunks) - 1:
        id_inds[i] = [ids[chunks[i]:chunks[i + 1]], i + 1]
        i += 1
    id_inds[i] = [ids[chunks[i]:], i]
    # program crashed after adding chunk 309 to db, restart from there
    id_inds = [id_inds[334], id_inds[394], id_inds[407]]
    # print(f"{len(id_inds)} chunks, starting from {id_inds[0][1]}, remain to be scraped.")

    mclient = pymongo.MongoClient()
    db = mclient.job_data_db
    descriptions = db.posts

    with get_context('spawn').Pool(3) as pool:
        for results in pool.imap_unordered(scrape_descr, id_inds):
            try:  # when restarting after a disconnection crash, inserts may be repeated. catch & ignore them.
                out = descriptions.insert_many(results, ordered=False)
            except pymongo.errors.BulkWriteError as e:
                print(f"ID {e.details['writeErrors'][0]['keyValue']['_id']} skipped as it is already in db.")
    pool.close()
    pool.terminate()
    print("Multiprocess context complete.")
    mclient.close()
