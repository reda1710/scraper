/*
SQL Structure:

db:
    prefs

tables:
    appliances
        appliance_id (int, primary key, auto-increment)
        user_id (int)
        appliance_name (varchar(255))
        appliance_type (varchar(255))
        status (varchar(50), default: 'Offline')
        wattage (int)

    appliance_daily_consumption
        appliance_id (int, foreign key)
        Columns representing hours from 00:00 to 23:00 (float, default: '0')

    appliance_recommended_hours
        appliance_id (int, foreign key)
        Columns representing hours from 00:00 to 23:00 (float, default: '0')

    appliance_user_prefs
        id (int, primary key, auto-increment)
        appliance_id (int, foreign key)
        start_time (time)
        end_time (time)

    appliance_weekly_consumption
        appliance_id (int, foreign key)
        Columns representing days from mon to sun (decimal(10,2), default: '0.00')

    daily_consumption
        user_id (int, primary key)
        Columns representing hours from 00:00 to 23:00 (float, default: '0')

    users
        ID (int, primary key, auto-increment)
        firstname (varchar(20))
        lastname (varchar(20))
        email (varchar(20), unique)
        phonenumber (varchar(20))
        password (text)
        todays_consumption (int)
        this_months_consumption (int)
        country (varchar(20))

    user_notifications
        id (int, primary key, auto-increment)
        user_id (int)
        message (text)
        created_at (timestamp, default: CURRENT_TIMESTAMP)
        is_read (tinyint(1), default: '0')
        alertType (varchar(10), default: 'dark')

    weekly_consumption
        user_id (int, primary key)
        Columns representing days from mon to sun (decimal(10,2))
*/

const { Kafka } = require('kafkajs');

// Kafka setup
const kafka = new Kafka({
    clientId: 'add-operation',
    brokers: ['localhost:9092']
});

const kafkaProducer = kafka.producer();

const payloads = [
    // Testing for appliances table
    {
        operation: 'insert',
        table: 'appliances',
        data: {
            user_id: 2,
            appliance_name: 'Washer',
            appliance_type: 'Utility',
            wattage: 100
        }
    },
    {
        operation: 'update',
        table: 'appliances',
        data: {
            set: { status: 'Online' },
            where: { appliance_id: 20 }
        }
    },
    {
        operation: 'delete',
        table: 'appliances',
        data: { appliance_id: 20 }
    },

    // Testing for appliance_daily_consumption table
    {
        operation: 'insert',
        table: 'appliance_daily_consumption',
        data: {
            appliance_id: 4,
            "00:00": 10,
            "01:00": 20
            // ... add other hours as needed
        }
    },
    {
        operation: 'update',
        table: 'appliance_daily_consumption',
        data: {
            set: { "01:00": 25 },
            where: { appliance_id: 4 }
        }
    },
    {
        operation: 'delete',
        table: 'appliance_daily_consumption',
        data: { appliance_id: 4 }
    },

    // Testing for appliance_recommended_hours table
    {
        operation: 'insert',
        table: 'appliance_recommended_hours',
        data: {
            appliance_id: 4,
            "00:00": 5,
            "01:00": 10
            // ... add other hours as needed
        }
    },
    {
        operation: 'update',
        table: 'appliance_recommended_hours',
        data: {
            set: { "01:00": 15 },
            where: { appliance_id: 4 }
        }
    },
    {
        operation: 'delete',
        table: 'appliance_recommended_hours',
        data: { appliance_id: 4 }
    },

    // Testing for appliance_user_prefs table
    {
        operation: 'insert',
        table: 'appliance_user_prefs',
        data: {
            appliance_id: 4,
            start_time: '06:00:00',
            end_time: '09:00:00'
        }
    },
    {
        operation: 'update',
        table: 'appliance_user_prefs',
        data: {
            set: { start_time: '07:00:00' },
            where: { id: 5 }
        }
    },
    {
        operation: 'delete',
        table: 'appliance_user_prefs',
        data: { id: 5 }
    },

    // Testing for appliance_weekly_consumption table
    {
        operation: 'insert',
        table: 'appliance_weekly_consumption',
        data: {
            appliance_id: 4,
            mon: '10.00',
            tue: '15.00',
            // ... add other days as needed
        }
    },
    {
        operation: 'update',
        table: 'appliance_weekly_consumption',
        data: {
            set: { tue: '20.00' },
            where: { appliance_id: 4 }
        }
    },
    {
        operation: 'delete',
        table: 'appliance_weekly_consumption',
        data: { appliance_id: 4 }
    },

    // Testing for users table
    {
        operation: 'insert',
        table: 'users',
        data: {
            firstname: 'Jane',
            lastname: 'Smith',
            email: 'jane.smith@example.com',
            phonenumber: '0987654321',
            password: 'password456',
            todays_consumption: 0,
            this_months_consumption: 0,
            country: 'UK'
        }
    },
    {
        operation: 'update',
        table: 'users',
        data: {
            set: { country: 'France' },
            where: { ID: 3 }
        }
    },
    {
        operation: 'delete',
        table: 'users',
        data: { ID: 3 }
    },

    // Testing for user_notifications table
    {
        operation: 'insert',
        table: 'user_notifications',
        data: {
            user_id: 2,
            message: 'Welcome Jane Smith!',
            alertType: 'info'
        }
    },
    {
        operation: 'update',
        table: 'user_notifications',
        data: {
            set: { is_read: 1 },
            where: { id: 10 }
        }
    },
    {
        operation: 'delete',
        table: 'user_notifications',
        data: { id: 10 }
    },

    // Testing for daily_consumption table
    {
        operation: 'insert',
        table: 'daily_consumption',
        data: {
            user_id: 3,
            "00:00": 25,
            "01:00": 30
            // ... add other hours as needed
        }
    },
    {
        operation: 'update',
        table: 'daily_consumption',
        data: {
            set: { "01:00": 35 },
            where: { user_id: 3 }
        }
    },
    {
        operation: 'delete',
        table: 'daily_consumption',
        data: { user_id: 3 }
    },

    // Testing for weekly_consumption table
    {
        operation: 'insert',
        table: 'weekly_consumption',
        data: {
            user_id: 3,
            mon: '50.50',
            tue: '75.25',
            // ... add other days as needed
        }
    },
    {
        operation: 'update',
        table: 'weekly_consumption',
        data: {
            set: { tue: '80.25' },
            where: { user_id: 3 }
        }
    },
    {
        operation: 'delete',
        table: 'weekly_consumption',
        data: { user_id: 3 }
    }
];

const runProducer = async () => {
    await kafkaProducer.connect();

    for(let payload of payloads) {
        await kafkaProducer.send({
            topic: 'DbOperations',
            messages: [{ value: JSON.stringify(payload) }]
        });
        console.log(`Sent payload for operation: ${payload.operation} on table: ${payload.table}`);
    }

    await kafkaProducer.disconnect();
};

runProducer().catch(console.error);
