require('dotenv').config();
const { PubSub } = require('@google-cloud/pubsub');

const pubsub = new PubSub();

const createTopic = async (topicName) => {
  pubsub
    .createTopic(topicName)
    .then(result => console.log(result))
    .catch(err => console.error('ERROR: ', err));
}

const createSubscription = async () => {
  await pubsub
    .createSubscription(process.env.NEW_TOPIC_GCP, process.env.NEW_SUBSCRIPTION_GCP)
    .then(result => {
      console.log(result);
      return {
        subscription: result[0],
        response: result[1]
      }
    })
    .catch(err => console.error('ERROR - createSubscription: ', err));
}

const getSubscriptonsTopic = async (topicName) => {
  const topic = pubsub.topic(topicName);

  return topic
    .getSubscriptions()
    .then(result => result[0])
    .catch(err => {
      console.error('ERROR: ', err);
      throw error;
    });
}

const validateSubscriptionExist = async (nameSubscription) => {
  const subscriptions = await getSubscriptonsTopic(process.env.NEW_TOPIC_GCP);

  return subscriptions.find(subs => (
    subs.name.replace('projects/my-project-july-2021/subscriptions/', '') === nameSubscription
  ));
}

const publishMessage = (message) => {
  const data = JSON.stringify(message);
  const dataBuffer = Buffer.from(data);
  const topic = pubsub.topic(process.env.NEW_TOPIC_GCP);

  console.log(`message: `, message);
  console.log(`data: `, data);

  topic
    .publish(dataBuffer)
    .then(result => console.log(`Message ${result} published.`))
    .catch(err => {
      console.error('ERROR: ', err);
      throw error;
    });
}

const pullMessageSubscription = () => {
  const topicName = process.env.NEW_TOPIC_GCP;
  const subscriptionName = process.env.NEW_SUBSCRIPTION_GCP;
  const subscription = pubsub.subscription(subscriptionName);

  const messageHandler = (message) => {
    console.log(`Received message ${message.id}: `);
    console.log(`Data: ${message.data}`);
    console.log(`tAttributes: ${JSON.stringify(message.attributes)}`);

    message.ack();
  };

  subscription.on('message', messageHandler);
  console.log(`Listening to ${topicName} with subscription ${subscriptionName}`);
}

module.exports = {
  publishMessage,
  pullMessageSubscription,
  validateSubscriptionExist
}
