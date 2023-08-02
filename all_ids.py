import json
import time
import random
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


def get_id_file(file: str):
    path = Path(file)
    if path.is_file():
        with open(path, 'r') as f:
            return json.load(f)
    return {}


def go_to(url, pg):
    while True:
        try:
            pg.goto(url, timeout=30000)
        except PlaywrightTimeoutError:
            print(f"Timeout error to {url}, attempting reload...", flush=True)
            continue
        break


def block_requests(rt):  # if uBlock Origin says I don't need these, I don't think the scraper does either
    block = {'t.indeed.com': True, 'secure.indeed.com': True, 'static.cloudflareinsights.com': True,
             'pt.ispot.tv': True}
    if block.get(rt.request.headers['host']):
        rt.abort()
    else:
        rt.continue_()


class IDScraper:
    '''
    Making the scraper a class with a method to start or stop the playwright context is much neater than throwing all the \
    code under a `with sync_playwright as p:` indent
    '''
    def __init__(self):
        self.ids1 = get_id_file("Scraping Practice/job_ids.json")
        self.ids2 = get_id_file("job_ids2_453491.json")  # whatever the most recent job_ids file is
        self.out = "job_ids2.json"
        self.add = 0
        self.p = None
        self.browser = None

    def set_playwright(self, instr: str):
        match instr:
            case 'start':
                self.p = sync_playwright().start()
                self.browser = self.p.firefox.launch()
            case 'stop':
                self.browser.close()
                self.p.stop()

    def write_all_jobs(self):
        with open(self.out, "w") as f:
            json.dump(self.ids2, f)

    def job_search(self, title):
        tag = "+".join(title.split()[:-1])
        cont = self.browser.new_context(record_har_content="omit")
        page = cont.new_page()
        page.route("", block_requests)
        url = "https://www.indeed.com/jobs?q=" + tag
        go_to(url, page)
        self.get_ids(page, title)
        npage = 10
        while page.query_selector(
                '//div[@class="jobsearch-LeftPane"]/nav/div/a[@data-testid="pagination-page-next"]'):
            if self.add > 20000:
                with open(f"job_ids2_{len(self.ids2)}.json", "w") as f:
                    json.dump(self.ids2, f)
                self.add = 0
            if npage > 190 and npage % 200 == 0:
                cont.close()
                cont = self.browser.new_context(record_har_content="omit")
                page = cont.new_page()
                page.route("", block_requests)
            go_to(f"{url}&start={npage}", page)
            time.sleep(random.uniform(0.25, 2.75))
            self.get_ids(page, title)
            npage += 10
        cont.close()

    def get_ids(self, pg, title):
        jobs = pg.query_selector_all('//td[@class="resultContent"]//a[@data-jk]')
        if len(jobs) > 0:
            for job in jobs:
                job_id = job.get_attribute('data-jk')
                if not self.ids1.get(job_id) and not self.ids2.get(job_id):
                    self.ids2.update({job_id: True})
                    self.add += 1
        else:
            print(f"Error loading search for {title}.")
            print("Page dump:\n")
            print(pg.title())
            try:
                print(pg.query_selector('.jobsearch-SerpMainContent', strict=False).inner_text())
            except:
                print("Error: no page main content.")

    def scrape_ids(self):
        chars = "ABDCEFGHIJKLMNOPQRSTUVWXYZ"
        init_cont = self.browser.new_context(record_har_content="omit")
        init_page = init_cont.new_page()
        init_page.route("", block_requests)
        for char in chars:
            go_to("https://www.indeed.com/browsejobs/Title/"+char, init_page)
            cats = init_page.query_selector_all('//p[@class="job"]/a[@class="jobTitle text_level_3"]')
            if len(cats) > 50:
                cats = cats[:50]
            for cat in cats:
                title = cat.get_attribute('title')
                print(f"searching for {title}")
                self.job_search(title)
        init_cont.close()


if __name__ == "__main__":
    scraper = IDScraper()
    scraper.set_playwright('start')
    scraper.scrape_ids()
    scraper.set_playwright('stop')
    scraper.write_all_jobs()
