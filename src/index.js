require('dotenv').config();
const app = require('express')();
const router = require('express').Router();
const morgan = require('morgan');

const { validateSubscriptionExist, publishMessage, pullMessageSubscription } = require('./gcp-pubsub');

const PORT = process.env.PORT;

// Loggeer Morgan
app.use(morgan('dev'));

// Json
app.use(require('express').json({limit: "50mb"}));
app.use(
  require('express').urlencoded({
    extended: true
  })
);

router.post('/publish-message-gcp', (req, res) => {
  const message = req.body ? req.body.message : null;

  if (message) {
    console.log(`Received message ${message}`);
    publishMessage(message);
  }

  return res.status(204).send();
});

app.use("/", router);

// Middleware Not Found
app.use((req, res, next) => {
  res.status(404).json("Resources Not Found")
});

app.listen(PORT, async () => {
  const subs = await validateSubscriptionExist(process.env.NEW_SUBSCRIPTION_GCP);

  if (subs) {
    pullMessageSubscription();
  }

  console.log(`Server listening to port ${PORT}`);
});
