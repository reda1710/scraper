const puppeteer = require('puppeteer');
const axios = require('axios');
const { Kafka } = require('kafkajs');
const cron = require('node-cron');

// Function to format a date as 'dd.MM.yyyy'
function formatDate(date) {
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0'); // Month is zero-based
    const year = date.getFullYear();
    return `${day}.${month}.${year}`;
}

// Function to scrape and send data
async function scrapeAndSendData() {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();

    const combinedData = { current_day: {}, day_after: {} };

    try {
        // Loop through two dates: current day and the day after
        for (let i = 0; i < 2; i++) {
            const currentDate = new Date();
            currentDate.setDate(currentDate.getDate() + i);
            const targetUrl = constructUrl(currentDate);
            await page.goto(targetUrl);

            // Conditional check for the close button
            const closeButtonExists = await page.$('#close-button');
            if (closeButtonExists) {
                await closeButtonExists.click();
            }

            await page.waitForSelector('.data-view-table');

            const data = await page.evaluate(() => {
                const rows = Array.from(document.querySelectorAll('.data-view-table tbody tr'));
                return rows.map(row => {
                    const mtu = row.querySelector('.first').textContent.trim();
                    const price = parseFloat(row.querySelector('.dv-value-cell span').textContent.trim());
                    return { mtu, price };
                });
            });

            const transformedData = data.map(item => {
                const hour = item.mtu.split(' ')[0];
                const result = {};
                result[hour] = item.price;
                return result;
            });

            // Accumulate data based on the current iteration
            if (i === 0) {
                combinedData.current_day = transformedData;
            } else {
                combinedData.day_after = transformedData;
            }
        }

        await sendDataToApi(combinedData);
        sendDataToKafka(combinedData);
        console.log('Web scraping script completed.');
    } catch (error) {
        console.error('Error during scraping:', error.message);
    } finally {
        await browser.close();
    }
}

// Function to send scraped data to the API
async function sendDataToApi(data) {
    try {
        await axios.post('http://localhost:3010/api/scraped-data', { data });
        console.log('Sent data to API:', data);
    } catch (error) {
        console.error('Error sending data to API:', error);
    }
}

// Function to send data to Kafka
async function sendDataToKafka(data) {
    const kafka = new Kafka({
        clientId: 'my-producer',
        brokers: ['localhost:9092'], // Your Kafka broker(s)
    });

    const producer = kafka.producer();

    try {
        await producer.connect();

        // Define Kafka topic to send messages to
        const kafkaTopic = 'price'; // Your desired Kafka topic

        // Send each scraped data point to Kafka
        for (const item of data) {
            const message = {
                value: JSON.stringify(item),
            };

            await producer.send({
                topic: kafkaTopic,
                messages: [message],
            });

            console.log(`Sent data to Kafka: ${JSON.stringify(item)}`);
        }
    } catch (error) {
        console.error('Error sending data to Kafka:', error);
    } finally {
        // Close the Kafka producer
        await producer.disconnect();
    }
}

// Construct the full URL
function constructUrl(date) {
    const baseUrl = 'https://transparency.entsoe.eu/transmission-domain/r2/dayAheadPrices/show';
    const queryParams = {
        name: '',
        defaultValue: false,
        viewType: 'TABLE',
        areaType: 'BZN',
        atch: false,
        'dateTime.dateTime': `${formatDate(date)} 00:00|CET|DAY`,
        'biddingZone.values': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A788',
        'resolution.values': 'PT60M',
        'dateTime.timezone': 'CET_CEST',
        'dateTime.timezone_input': 'CET (UTC+1) / CEST (UTC+2)'
    };

    const url = new URL(baseUrl);
    Object.keys(queryParams).forEach(key => {
        url.searchParams.append(key, queryParams[key]);
    });

    return url.toString();
}

// Schedule the scraping task to run daily at a specific time (e.g., 2:00 AM)
cron.schedule('00 00 * * *', async () => {
    console.log('Running the web scraping script...');
    await scrapeAndSendData();
});

// Initial execution (optional)
console.log('Starting the web scraping script...');
scrapeAndSendData();
