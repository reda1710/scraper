const { Kafka } = require('kafkajs');
const mysql = require('mysql');

// MySQL connection setup
const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'prefs'
});

// Kafka setup
const kafka = new Kafka({
    clientId: 'manage_db',
    brokers: ['localhost:9092']
});

const kafkaConsumer = kafka.consumer({ groupId: 'manage_db' });
const kafkaProducer = kafka.producer();

db.connect((err) => {
    if(err) throw err;
    console.log('MySQL Connected...');
});

const run = async () => {
    await kafkaConsumer.connect();
    await kafkaConsumer.subscribe({ topic: 'DbOperations', fromBeginning: true });
    await kafkaConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const payload = JSON.parse(message.value.toString());
            handleDbOperation(payload.operation, payload.table, payload.data);
        },
    });
    await kafkaProducer.connect();
};

const sendResponseToKafka = async (topic, message) => {
    await kafkaProducer.send({
        topic: topic,
        messages: [{ value: JSON.stringify(message) }],
    });
};

const handleDbQuery = (operation, table, data) => {
    let sql = '';
    let queryParams = [];

    switch(operation) {
        case 'insert':
            sql = `INSERT INTO ${table} SET ?`;
            queryParams = [data];
            break;
        case 'delete':
            const primaryKey = getPrimaryKeyForTable(table);
            sql = `DELETE FROM ${table} WHERE ${primaryKey} = ?`;
            queryParams = [data[primaryKey]];
            break;
        case 'update':
            const updateKey = getPrimaryKeyForTable(table);
            sql = `UPDATE ${table} SET ? WHERE ${updateKey} = ?`;
            queryParams = [data.set, data.where[updateKey]];
            break;
    }

    if(sql) {
        db.query(sql, queryParams, (err, result) => {
            if(err) {
                console.error(`Error during operation: ${operation} on table: ${table}.`, err);
                sendResponseToKafka('DbOperationsErrors', {
                    operation,
                    table,
                    success: false,
                    error: err.message
                });
                return;
            }
            console.log(`Operation: ${operation} on table: ${table} completed.`);
            sendResponseToKafka('DbOperationsResponses', {
                operation,
                table,
                success: true
            });
        });
    } else {
        console.log('Unknown operation or table.');
        sendResponseToKafka('DbOperationsErrors', {
            operation,
            table,
            success: false,
            error: 'Unknown operation or table.'
        });
    }
};

const getPrimaryKeyForTable = (table) => {
    const primaryKeyMap = {
        'appliances': 'appliance_id',
        'appliance_daily_consumption': 'appliance_id',
        'appliance_recommended_hours': 'appliance_id',
        'appliance_user_prefs': 'id',
        'appliance_weekly_consumption': 'appliance_id',
        'daily_consumption': 'user_id',
        'users': 'ID',
        'user_notifications': 'id',
        'weekly_consumption': 'user_id'
    };
    return primaryKeyMap[table];
};

const handleFetchOperation = (table, criteria) => {
    let sql = `SELECT * FROM ${table}`;

    if(criteria && Object.keys(criteria).length) {
        const whereClauses = Object.entries(criteria).map(([key, value]) => `${key} = ${db.escape(value)}`).join(' AND ');
        sql += ` WHERE ${whereClauses}`;
    }

    db.query(sql, (err, results) => {
        if(err) throw err;

        const responsePayload = {
            table: table,
            data: results
        };
        sendResponseToKafka('DbOperationsResponses', responsePayload);
    });
};

const handleDbOperation = (operation, table, data) => {
    switch(operation) {
        case 'fetch':
            handleFetchOperation(table, data);
            break;
        default:
            handleDbQuery(operation, table, data);
            break;
    }
};

run().catch(console.error);
