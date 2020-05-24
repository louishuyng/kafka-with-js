const { Kafka } = require("kafkajs");

run();

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "mykafkaApp",
      brokers: ["louis:9092"],
    });
    const consumer = kafka.consumer({ groupId: "testproducer" });
    console.log("Connecting........");

    await consumer.connect();
    console.log("Connected.........");
    consumer.subscribe({
      topic: "Users",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(
          ` RVD Message ${result.message.value} on partition ${result.partition}`
        );
      },
    });
  } catch (er) {
    console.log(`Some thing bad happend ${er}`);
  } finally {
  }
}
