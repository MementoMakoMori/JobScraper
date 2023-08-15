const playwright = require('playwright');
const { MongoClient } = require('mongodb');
const fs = require('fs');
let data = require('./job_ids_list.json');

// check if prev_data exists, creates it if not
if (fs.existsSync('prev_data.json')) {
    var prev_data = require('./prev_data.json');
} else {
    fs.writeFileSync('./prev_data.json', JSON.stringify({total: 0, id_ind: 0, to_send: []}));
    var prev_data = require('./prev_data.json');
}


// artificially slow down the scraper
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


// this function grabs the data for each job_id
async function main(id, page) {

    let url = 'https://www.indeed.com/viewjob?jk='.concat("", id)

    await page.goto(url, {waitUntil: "load"});
    const title = await page.title();
    if (title.includes('Page Not Found')) {
        console.log(`Job ${id} no longer exists.`)
        return null;
    }

    const job = await page.$eval('.jobsearch-JobComponent', (jobinfo) => {
        const occ = jobinfo.querySelector('.jobsearch-JobInfoHeader-title-container');
        const org = jobinfo.querySelector('.css-1cjkto6');
        const loc = jobinfo.querySelector('.css-6z8o9s');
        const descr = jobinfo.querySelector('#jobDescriptionText');

        const toText = (element) => element && element.innerText.trim();

        return {
            occ: toText(occ),
            org: toText(org),
            loc: toText(loc),
            descr: descr.innerHTML
        };

    });

    job._id = id;
    // console.log(`Found job descr of length ${job.descr.length}`);
    return job;

}

async function scrape() {

    let browser = await playwright.firefox.launch({headless: true});
    let page = await browser.newPage({bypassCSP: true,});

    while (prev_data.to_send.length < 25){
        
        //update prev_data id_ind
        let id = data[prev_data.id_ind];
        await fs.writeFileSync('./prev_data.json', JSON.stringify(prev_data));
        
        let result = null;

        try {
            result = await main(id, page);
        } catch (error) {
            console.log(`Error for id ${id}`);
            try {
                await browser.close()
                browser = await playwright.firefox.launch({headless: true});
                page = await browser.newPage({bypassCSP: true,});
                result = await main(id, page);
            } catch (error) {
                console.log('Failed on 2nd attempt.')
            }
        }
        if (result !== null) {
            //console.log('Adding results to prev_data.to_send')
            prev_data.to_send.push(result);
        }
        //console.log('Incrementing prev_data.id_ind and writing updates to file.')
        prev_data.id_ind ++;
        await fs.writeFileSync('./prev_data.json', JSON.stringify(prev_data));
        if (prev_data.id_ind === data.length) { break; }
    }
    //console.log('Finished scraping chunk.')
    await browser.close();

    // connect to and insert jobs into database
    const uri = 'mongodb://127.0.0.1:27017';
    const client = new MongoClient(uri);
    const db = client.db('job_data_db');
    const test_coll = db.collection('august_jobs');

    //console.log('Attempt db insert...')
    await test_coll.insertMany(prev_data.to_send);
    prev_data.total = prev_data.total + prev_data.to_send.length;
    prev_data.to_send = [];
    await fs.writeFileSync('./prev_data.json', JSON.stringify(prev_data));
    //console.log('Insert completed, prev_data.json updated.')
    
    client.close();

    // get updates in the terminal every 1000 jobs
    if ((prev_data.total > 1000) && (prev_data.total%1000 == 0)) {
        console.log(`${prev_data.total} jobs added to database.`)
    }
}

async function run() {
    console.log('Starting scraper...')
    while (prev_data.id_ind !== data.length) {
        await scrape();
        await sleep(1000);
    }
}

run();


