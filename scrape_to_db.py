import json
import time
import pymongo
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from multiprocessing import get_context, Pool
import sys
from datetime import datetime

'''
Q: Why are there so many print statements, and sys.stdout is assigned to a file?
A: logging multiprocessesing is a PITA.
the multiprocessing_logging package only works with forks, but pymongo connection is not is not fork safe 
(thus I use 'spawn' for the context)
'''

# set stdout to a file automatically named by date
sys.stdout = open("scrape_to_db" + datetime.now().strftime("%m%d") + ".out", "a")


def scrape_descr(job_ids: list):
    with sync_playwright() as p:
        browser = p.firefox.launch()  # launch(headless=False, slow_mo=500) for debugging
        cont = browser.new_context(record_har_content="omit")
        page = cont.new_page()
        total = [] # list to collect all data this chunk returns
        chunkn = job_ids[1]
        job_ids = job_ids[0]

        def block_requests(rt):  # if uBlock Origin says I don't need these, then I don't think the scraper does either
            block = {'t.indeed.com': True, 'secure.indeed.com': True, 'static.cloudflareinsights.com': True,
                     'pt.ispot.tv': True}
            if block.get(rt.request.headers['host']):
                rt.abort()
            else:
                rt.continue_()

        def go_to(url):  # goto is wrapped in this function so a timeout will attempt to reload instead of giving up
            nonlocal page
            while True:
                try:
                    page.goto(url, timeout=30000, wait_until="load")
                except PlaywrightTimeoutError:
                    print(f"Chunk {chunkn}: Timeout error to {url}, attempting reload...", flush=True)
                    continue
                break

        def scrape(job_id):
            nonlocal page
            go_to(f'https://www.indeed.com/viewjob?jk={job_id}')
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
            if len(body) < 20000:
                return {"_id": job_id, "occ": occ, "org": org, "loc": loc, "descr": body}

        page.route("", block_requests)
        for num in range(0, len(job_ids)):
            res = scrape(job_ids[num])
            if res:
                total.append(res)
            if num % 25 == 0 and num >= 25:  # restart context every 25 pages to avoid bot defenses
                cont.close()
                cont = browser.new_context(record_har_content="omit")
                page = cont.new_page()
                page.route("", block_requests)
        cont.close()
        browser.close()
    print(f"Sending chunk {chunkn} to results.", flush=True)
    return total


if __name__ == '__main__':
    with open('../job_ids2_453491.json', 'r') as f:  # newly scraped job ids
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
    mclient = pymongo.MongoClient()
    db = mclient.job_data_db
    descriptions = db.big_jobs

    '''
    this is the section where a pool of workers take chunks of ids, scrape the data, and insert them into the mongodb
    the pool was terminating before any results were inserted due to an 'invalid state' error
    likely caused by the playwright firefox browser connection error
    so the following part is similar code that runs sequentially instead of using a processing pool
    '''
    # with get_context('spawn').Pool(3) as pool:
    #     for results in pool.imap_unordered(scrape_descr, id_inds):
    #         try:  # when restarting after a disconnection crash, inserts may be repeated. catch & ignore them.
    #             out = descriptions.insert_many(results, ordered=False)
    #             print(out)
    #         except pymongo.errors.BulkWriteError as e:
    #             print(f"ID {e.details['writeErrors'][0]['keyValue']['_id']} skipped as it is already in db.")
    # print("Multiprocess context complete.")

    '''
    this section was to test if the scraping would work sequentially
    but no
    it doesn't
    it's still playwright's firefox browser connection error
    and I can't use chromium or webkit browsers because they trigger bot defenses
    '''
    for results in map(scrape_descr, id_inds):
        print(f'Returned {len(results)}.', flush=True)
        # try:  # when restarting after a disconnection crash, inserts may be repeated. catch & ignore them.
        #     out = descriptions.insert_many(results, ordered=False)
        #     print(out)
        # except pymongo.errors.BulkWriteError as e:
        #     print(f"ID {e.details['writeErrors'][0]['keyValue']['_id']} skipped as it is already in db.")
    # turns out multiprocess wasn't the issue, it's still the playwright firefox connection causing errors! argh!
    mclient.close()
