import { Kafka } from 'kafkajs';
import {createClient} from 'redis';
import {getLocalBroker, getTopic} from "./config/config.js";
import {redisOptions} from "./config/configRedis.js";

const isLocalBroker = getLocalBroker();

const client = await createClient(redisOptions);
client.on('error', err => console.log('Redis Client Error', err));

await client.connect();

const redpanda = new Kafka({
    brokers: [
        isLocalBroker ? `${process.env.HOST_IP}:9092` : 'redpanda-0:9092',
        'localhost:19092'
    ]
});

const consumer = redpanda.consumer({groupId: 'mon-super-groupe'});

connexion()

async function connexion() {
    try {
        await consumer.connect();
        await consumer.subscribe({ topic: getTopic(), fromBeginning: true });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const messageJson = JSON.parse(message.value)

                const tabMot = messageJson.message.split(" ")

                tabMot.forEach((mot) => {
                    client.incr(mot, (err, newValue) => {
                        if (err) {
                            console.error('Erreur lors de l\'incrémentation :', err);
                        } else {
                            console.log('Nouvelle valeur incrémentée :', newValue);
                        }
                    });
                });

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
