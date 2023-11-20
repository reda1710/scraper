const express = require('express');
const cors = require('cors'); // Import the cors middleware
const app = express();
const port = 3010;

// Initialize an empty variable to store the scraped data
let scrapedData = null;

// Configure middleware to parse JSON requests
app.use(express.json());

// Use the cors middleware
app.use(cors());

// Define an API endpoint to receive the scraped data via POST request
app.post('/api/scraped-data', (req, res) => {
    scrapedData = req.body.data;
    res.sendStatus(200); // Respond with a success status code (e.g., 200 OK)
});

// Define an API endpoint to get the scraped data
app.get('/api/scraped-data', (req, res) => {
    if (scrapedData !== null) {
        res.json(scrapedData);
    } else {
        res.status(404).json({ error: 'Data not found' });
    }
});

app.listen(port, () => {
    console.log(`API is listening on port ${port}`);
});
