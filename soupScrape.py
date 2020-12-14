from bs4 import BeautifulSoup as bs
import requests
import nltk

## generate Indeed.com search urls according to desired number of pages
## preset seach terms are keyword: 'data scientist' and experience: 'entry level'
def searchURL(npage: int) -> list:
    url="https://www.indeed.com/jobs?q=data%20science&explvl=entry_level&start="
    if npage >= 2:
        nums = list(range(0,((npage-1)*15)+1, 15))
        listPages = []
        for i in nums:
            listPages.append("".join([url, str(i)]))
    else: listPages = "".join([url, '0'])

    sources = [0] * len(listPages)
    jobids = []
    for i in range(len(listPages)):
        sources[i] = requests.get(listPages[i])
        sources[i] = bs(sources[i].content, 'html.parser')
        sources[i] = sources[i].find_all(class_="jobsearch-SerpJobCard unifiedRow row result")
        for j in sources[i]:
            jobids.append(j['data-jk'])
    ## remove duplicate job posts by casting to set type
    jobids = set(jobids)
    ## generate job frame urls
    jobURL = []
    for k in jobids:
        jobURL.append("".join(["https://www.indeed.com/viewjob?viewtype=embedded&jk=", k]))
    return jobURL

## grab descriptions from each job post
def getDescr(pages: list) -> list:
    descr = [0] * len(pages)
    for job in range(len(pages)):
        descr[job] = requests.get(pages[job])
        descr[job] = bs(descr[job].content, 'html.parser')
        descr[job] = descr[job].find(id='jobDescriptionText')
        descr[job] = descr[job].get_text()
    return descr

## clean text and basic processing
def testClean(string):

    words = nltk.word_tokenize(string)
    words = nltk.Text(words)
    return words

samp_words = nltk.word_tokenize(samp[0])
fdist1 = nltk.FreqDist(samp_words)
fdist1.plot(50, cumulative = True)
