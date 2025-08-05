const express = require('express');
const axios = require('axios');
const { Sequelize, DataTypes, Op } = require('sequelize');
const { Parser } = require('json2csv');
const fs = require('fs');
const path = require('path');

// --- CONFIGURATION ---
const app = express();
const PORT = process.env.PORT || 3000; // Use Render's port or 3000 for local dev

const fdaApiBaseUrl = "https://api.fda.gov/device/";
const apiEndpoints = {
    pma: "pma.json",
    '510k': "510k.json"
};
const fetchBatchSize = 100;

const databaseFileName = "fda_devices.db";
const sequelize = new Sequelize({
    dialect: 'sqlite',
    storage: path.join(__dirname, databaseFileName),
    logging: false,
});

const decisionStatusMap = {
    "SE": "Cleared", "NSE": "Rejected", "SGC": "Cleared",
    "AP": "Approved", "APPR": "Approved", "DE": "Denied",
    "WD": "Withdrawn", "CD": "Conditional Approval"
};

// --- DATABASE MODEL ---
const Device = sequelize.define('Device', {
    submissionNumber: { type: DataTypes.STRING, allowNull: false, unique: true, primaryKey: true, field: 'submission_number' },
    deviceName: { type: DataTypes.STRING, field: 'device_name' },
    submissionType: { type: DataTypes.STRING, field: 'submission_type' },
    decision: DataTypes.STRING,
    decisionDate: { type: DataTypes.DATEONLY, field: 'decision_date' },
    applicant: DataTypes.STRING,
    productCode: { type: DataTypes.STRING, field: 'product_code' },
    regulationNumber: { type: DataTypes.STRING, field: 'regulation_number' }
}, {
    tableName: 'devices',
    timestamps: false
});

// --- CORE LOGIC ---
async function initializeDatabase() {
    console.log(`Connecting to database: ${databaseFileName}`);
    await sequelize.sync();
    console.log("Database synchronized successfully.");
}

async function fetchDeviceData(submissionType, filterQuery = null, maxRecords = 100) {
    const fetchedRecords = [];
    let skip = 0;
    const endpoint = apiEndpoints[submissionType];
    const requestUrl = fdaApiBaseUrl + endpoint;

    console.log(`Starting data fetch from ${requestUrl}`);
    while (fetchedRecords.length < maxRecords) {
        const batchLimit = Math.min(fetchBatchSize, maxRecords - fetchedRecords.length);
        const requestParams = { limit: batchLimit, skip };
        if (filterQuery) { requestParams.search = filterQuery; }

        try {
            const response = await axios.get(requestUrl, { params: requestParams });
            const records = response.data.results || [];
            if (records.length === 0) {
                console.log("API returned no more records. Ending fetch.");
                break;
            }
            fetchedRecords.push(...records);
            skip += records.length;
            console.log(`Fetched ${fetchedRecords.length} of ${maxRecords} total records...`);
        } catch (error) {
            console.error(`API request failed: ${error.message}`);
            break;
        }
    }
    console.log(`Total records retrieved for ${submissionType}: ${fetchedRecords.length}`);
    return fetchedRecords;
}

function transformApiData(records, type) {
    return records.map(record => {
        let deviceData;
        try {
            if (type.toLowerCase() === 'pma') {
                deviceData = {
                    submissionNumber: record.pma_number, deviceName: record.device_name, submissionType: 'PMA',
                    decision: decisionStatusMap[record.decision_code] || 'Unknown', decisionDate: record.decision_date || null,
                    applicant: record.applicant, productCode: record.product_code,
                };
            } else if (type.toLowerCase() === '510k') {
                deviceData = {
                    submissionNumber: record.k_number, deviceName: record.device_name, submissionType: '510k',
                    decision: decisionStatusMap[record.decision_code] || 'Unknown', decisionDate: record.decision_date || null,
                    applicant: record.applicant, productCode: record.product_code, regulationNumber: record.regulation_number
                };
            }
            return deviceData.submissionNumber ? deviceData : null;
        } catch (error) {
            console.warn(`Skipping a record due to a parsing error.`);
            return null;
        }
    }).filter(Boolean);
}

async function storeDeviceData(deviceRecords) {
    if (deviceRecords.length === 0) {
        console.log("No new records to save.");
        return;
    }
    try {
        const result = await Device.bulkCreate(deviceRecords, { ignoreDuplicates: true });
        console.log(`Database store complete. Added: ${result.length}, Skipped (duplicates): ${deviceRecords.length - result.length}`);
    } catch (error) {
        console.error("Failed during bulk database insert:", error);
    }
}

// --- WEB SERVER ENDPOINTS ---

app.get('/', (req, res) => {
    res.send('FDA Device Importer is running. Navigate to /export/csv to generate and download the report.');
});

app.get('/export/csv', async (req, res) => {
    try {
        console.log("Received request to generate CSV export.");
        
        // Step 1: Initialize DB
        await initializeDatabase();

        // Step 2: Fetch PMA data
        console.log("\n[Step 1] Fetching recent PMA decisions...");
        const rawPmaRecords = await fetchDeviceData('pma', null, 50);
        const pmaDevices = transformApiData(rawPmaRecords, 'pma');
        await storeDeviceData(pmaDevices);

        // Step 3: Fetch 510(k) data
        console.log("\n[Step 2] Fetching 510(k) clearances from the last 90 days...");
        const endDate = new Date();
        const startDate = new Date();
        startDate.setDate(endDate.getDate() - 90);
        const formatDate = (d) => d.toISOString().split('T')[0];
        const dateRangeQuery = `decision_date:[${formatDate(startDate)}+TO+${formatDate(endDate)}]`;
        const raw510kRecords = await fetchDeviceData('510k', dateRangeQuery, 5000);
        const k510Devices = transformApiData(raw510kRecords, '510k');
        await storeDeviceData(k510Devices);

        // Step 4: Query all data and generate CSV
        console.log("\n[Step 3] Generating CSV from database...");
        const allDevices = await Device.findAll({ raw: true });
        if (allDevices.length === 0) {
            return res.status(404).send("No data found in the database to export.");
        }

        const json2csvParser = new Parser();
        const csv = json2csvParser.parse(allDevices);

        // Step 5: Send the file as a download
        const fileName = `fda_device_export_${Date.now()}.csv`;
        res.header('Content-Type', 'text/csv');
        res.attachment(fileName);
        console.log(`\n[Step 4] Sending file '${fileName}' to user.`);
        return res.send(csv);

    } catch (error) {
        console.error("A critical error occurred during the export process:", error);
        res.status(500).send("An internal server error occurred while generating the report.");
    }
});

// --- START SERVER ---
app.listen(PORT, () => {
    console.log(`Server is listening on port ${PORT}`);
});
