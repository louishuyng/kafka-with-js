const { Kafka } = require("kafkajs");

const msg = process.agrv[2];

run();

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "mykafkaApp",
      brokers: ["louis:9092"],
    });
    // const admin = kafka.admin();
    const producer = kafka.producer();
    console.log("Connecting........");

    // await admin.connect();
    await producer.connect();
    console.log("Connected.........");

    const partition = msg[0] < "N";

    const result = await producer.send({
      topic: "Users",
      messages: [
        {
          value: msg,
          partition: partition,
        },
      ],
    });

    // admin.createTopics({
    //   topics: [
    //     {
    //       topic: "Users",
    //       numPartitions: 2,
    //     },
    //   ],
    // });

    console.log(`Send Successfully! ${JSON.stringify(result)}`);
    // await admin.disconnect();
    await producer.disconnect();
  } catch (er) {
    console.log(`Some thing bad happend ${er}`);
  } finally {
    process.exit();
  }
}
