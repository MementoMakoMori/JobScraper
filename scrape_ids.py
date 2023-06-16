# import asyncio
import json
import time
import csv
import random
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# only grabbing ids
def scrape_jobids(title: str = "jedi"):

    def get_ids():
        global ids, add
        jobs = page.query_selector_all('//td[@class="resultContent"]//a[@data-jk]')
        if len(jobs) > 0:
            for job in jobs:
                job_id = job.get_attribute('data-jk')
                if not ids.get(job_id):
                    ids.update({job_id: True})
                    add += 1
        else:
            res = page.query_selector('.jobsearch-NoResult-messageContainer')
            if res and "did not match any jobs." in res.inner_text():
                print(f"No results for {title} jobs.")
            else:
                print(f"Error loading search for {title} jobs.")
                print("Page dump:\n")
                print(page.title())
                try:
                    print(page.query_selector('.jobsearch-SerpMainContent', strict=False).inner_text())
                except:
                    print("Error: no page main content.")

    with sync_playwright() as p:
        browser = p.firefox.launch()
        page = browser.new_page()

        def go_to(url):
            nonlocal page, browser
            while True:
                try:
                    page.goto(url)
                except PlaywrightTimeoutError:
                    print("Timeout error, restarting browser...")
                    browser.close()
                    browser = p.firefox.launch()
                    page = browser.new_page()
                    continue
                break
            page.wait_for_load_state("load")

        go_to(f"https://www.indeed.com/jobs?q={title}")
        get_ids()
        npage = 10
        while page.query_selector('//div[@class="jobsearch-LeftPane"]/nav/div/a[@data-testid="pagination-page-next"]'):
            time.sleep(random.uniform(0.25, 2.75))
            go_to(f"https://www.indeed.com/jobs?q={title}&start={npage}")
            get_ids()
            npage += 10
        browser.close()


if __name__ == '__main__':
    path = Path('job_ids.json')
    if path.is_file():
        with open(path, 'r') as f:
            ids = json.load(f)
        f.close()
    else:
        ids = {}
    with open('../job_titles.csv', 'r') as f:  # using the occupations I grabbed from Wookieepedia as search terms
        job_titles = list(csv.reader(f))[0]
    f.close()
    add = 0
    for each in job_titles:
        print(f"Searching for {each} job posts.")
        scrape_jobids(title=each)
        if add > 20000:  # write to file every 20000 IDs so I have some data in case there is a crash
            n = len(ids)
            with open(f'ids_{n}.json', 'w') as f:
                json.dump(ids, f)
            f.close()
            add = 0
            print(f"Dumped ids at length: {n}.")
    # with open(path, 'w') as f:
    with open('job_ids.json', 'w') as f:
        json.dump(ids, f)
    f.close()
