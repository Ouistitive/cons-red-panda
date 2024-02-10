import { Kafka } from 'kafkajs';
import {getLocalBroker, getTopic} from "./config/config.js";

const isLocalBroker = getLocalBroker();

const redpanda = new Kafka({
    brokers: [
        isLocalBroker ? `${process.env.HOST_IP}:9092` : 'redpanda-0:9092',
        'localhost:19092'
    ]
});

const consumer = redpanda.consumer({groupId: 'test-group-7'});

connexion()

async function connexion() {
    try {
        await consumer.connect()
        await consumer.subscribe({ topic: getTopic(), fromBeginning: true })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const messageJson = JSON.parse(message.value)


                console.log({
                    value: messageJson.message,
                    timestamp: formatDateAndTime(parseInt(message.timestamp))
                });
            },
        })

    } catch (error) {
        console.error("Error:", error);
    }
}

function formatDateAndTime(milliseconds) {
    const dateObj = new Date(milliseconds);
    const day = String(dateObj.getDate()).padStart(2, '0');
    const month = String(dateObj.getMonth() + 1).padStart(2, '0');
    const year = dateObj.getFullYear();
    const hours = String(dateObj.getHours()).padStart(2, '0');
    const minutes = String(dateObj.getMinutes()).padStart(2, '0');

    return `${day}/${month}/${year} à ${hours}:${minutes}`;
}
