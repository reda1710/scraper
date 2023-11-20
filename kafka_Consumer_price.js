const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'consume-price',
    brokers: ['localhost:9092'], // Replace with your Kafka broker(s)
});

const consumer = kafka.consumer({ groupId: 'price' });

const kafkaTopic = 'price'; // Replace with your Kafka topic

const runConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Received message: ${message.value.toString()}`);
        },
    });
};

runConsumer().catch((error) => {
    console.error('Error running Kafka consumer:', error);
});
