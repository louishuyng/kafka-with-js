const { Kafka } = require("kafkajs");

const msg = process.agrv[2];

run();

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "mykafkaApp",
      brokers: ["louis:9092"],
    });
    const admin = kafka.admin();
    const producer = kafka.producer();
    console.log("Connecting........");

    await admin.connect();
    await producer.connect();
    console.log("Connected.........");

    producer.send({
      topic: "Users",
      messages: [
        {
          value: msg,
        },
      ],
    });

    admin.createTopics({
      topics: [
        {
          topic: "Users",
          numPartitions: 2,
        },
      ],
    });

    console.log("Created Successfully!");
    await admin.disconnect();
  } catch (er) {
    console.log(`Some thing bad happend ${er}`);
  } finally {
    process.exit();
  }
}
