import json
import time
import csv
import random
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# only grabbing ids
def scrape_jobids():
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ$"
    with sync_playwright() as p:
        init_browser = p.firefox.launch()
        init_cont = init_browser.new_context(record_har_content="omit")
        init_page = init_cont.new_page()

        def block_requests(rt):  # if uBlock Origin says I don't need these, then I don't think the scraper does either
            block = {'t.indeed.com': True, 'secure.indeed.com': True, 'static.cloudflareinsights.com': True,
                     'pt.ispot.tv': True}
            if block.get(rt.request.headers['host']):
                rt.abort()
            else:
                rt.continue_()

        def get_jobs(title: str):
            global add, ids2
            tag = "+".join(title.split()[:-1])
            browser = p.firefox.launch()
            cont = browser.new_context(record_har_content="omit")
            page = cont.new_page()
            page.route("", block_requests)
            n = 10
            url = "https://www.indeed.com/jobs?q=" + tag + "&sort=date"
            go_to(url, page)
            get_ids(page, title)
            npage = 10
            while page.query_selector(
                    '//div[@class="jobsearch-LeftPane"]/nav/div/a[@data-testid="pagination-page-next"]'):
                if add > 20000:
                    with open(f"job_ids2_{len(ids2)}.json", "w") as f:
                        json.dump(ids2, f)
                    add = 0
                if npage > 190 and npage % 200 == 0:
                    cont.close()
                    browser.close()
                    browser = p.firefox.launch()
                    cont = browser.new_context(record_har_content="omit")
                    page = cont.new_page()
                    page.route("", block_requests)
                time.sleep(random.uniform(0.25, 2.75))
                go_to(f"{url}&start={n}", page)
                get_ids(page, title)
                npage += 10

        def get_ids(page, title):
            global ids, ids2, add
            jobs = page.query_selector_all('//td[@class="resultContent"]//a[@data-jk]')
            if len(jobs) > 0:
                for job in jobs:
                    job_id = job.get_attribute('data-jk')
                    # if not ids.get(job_id):
                    #     ids.update({job_id: True})
                    if not ids.get(job_id) and not ids2.get(job_id):
                        ids2.update({job_id: True})
                        add += 1
            else:
                res = page.query_selector('.jobsearch-NoResult-messageContainer')
                if res and "did not match any jobs." in res.inner_text():
                    print(f"No results for {title}.")
                else:
                    print(f"Error loading search for {title}.")
                    print("Page dump:\n")
                    print(page.title())
                    try:
                        print(page.query_selector('.jobsearch-SerpMainContent', strict=False).inner_text())
                    except:
                        print("Error: no page main content.")

        def go_to(url, pg):
            while True:
                try:
                    pg.goto(url, timeout=30000)
                except PlaywrightTimeoutError:
                    print(f"Timeout error to {url}, attempting reload...")
                    continue
                break

        init_page.route("", block_requests)
        for char in chars:
            go_to("https://www.indeed.com/browsejobs/Title/"+char, init_page)
            cats = init_page.query_selector_all('//p[@class="job"]/a[@class="jobTitle text_level_3"]')
            for cat in cats:
                title = cat.get_attribute('title')
                print(f"searching for {title}")
                get_jobs(title)
    with open('job_ids_more.json', 'w') as f:
        json.dump(ids2, f)
        print(f"Saved {len(ids2)} to file job_ids_more.json")


if __name__ == '__main__':
    path = Path('job_ids.json')
    if path.is_file():
        with open(path, 'r') as f:
            ids = json.load(f)
    else:
        ids = {}
    path2 = Path('job_ids2.json')
    if path2.is_file():
        with open(path2, 'r') as f:
            ids2 = json.load(f)
    else:
        ids2 = {}
    add = 0
    scrape_jobids()


    # with open('../job_titles.csv', 'r') as f:  # using the occupations I grabbed from Wookieepedia as search terms
    #     job_titles = list(csv.reader(f))[0]
    # to_remove = ["Black Sun sector chief", "Imperial Saboteur", "Supreme Commander of the Alliance Armed Forces"]
    # for each in to_remove:
    #     job_titles.remove(each)
    # add = 0
    # for each in job_titles:
    #     print(f"Searching for {each} job posts.")
    #     scrape_jobids(title=each)
    #     if add > 20000:  # write to file every 20000 IDs so I have some data in case there is a crash
    #         n = len(ids)
    #         with open(f'ids2_{n}.json', 'w') as f:
    #             json.dump(ids2, f)
    #         f.close()
    #         add = 0
    #         print(f"Dumped ids at length: {n}.")
    # # with open(path, 'w') as f:
    # with open('job_ids2.json', 'w') as f:
    #     json.dump(ids, f)
    # f.close()
    # for each in to_remove:
    #     job_titles.remove(each)
