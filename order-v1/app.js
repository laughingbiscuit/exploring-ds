const express = require('express')
const Pulsar = require("pulsar-client");
const crypto = require("crypto");
const { Client } = require('cassandra-driver')
const app = express()
app.use(express.urlencoded({extended: true}))
const port = 3000

/* Messaging Setup */
const tokenStr = process.env.DS_PULSAR_TOKEN;
const pulsarUri = "pulsar+ssl://pulsar-gcp-europewest1.streaming.datastax.com:6651";
const topicName =
  "persistent://pizza/default/orders";
const subscriptionName = "test-subscription";
//const trustStore = '/etc/ssl/certs/ca-certificates.crt'
const auth = new Pulsar.AuthenticationToken({ token: tokenStr });

const pulsarClient = new Pulsar.Client({
  serviceUrl: pulsarUri,
  authentication: auth,
  // tlsTrustCertsFilePath: trustStore,
  operationTimeoutSeconds: 30,
});

/* DB Setup */
const client = new Client({
  cloud: {
    secureConnectBundle: "../scb.zip",
  },
  credentials: {
    username: process.env.DS_CLIENTID,
    password: process.env.DS_CLIENTSECRET
  },
});

/* API Endpoints */
app.get('/ping', (req, res) => {
  res.json({"status":"ok"})
})

app.post('/orders', async (req, res) => {
  const producer = await pulsarClient.createProducer({
    topic: topicName,
  });
  const query = 'INSERT INTO pizza.orders (id, name, addr1, village, order_time, pizza, status) '
    + 'VALUES(?, ?, ?, ?, ?, ?, ?)'
  const orderTime = new Date()
  const id = crypto.randomUUID();
  const orderDetail = [
      id,
      req.body.name,
      req.body.addr1,
      req.body.village,
      orderTime, 
      req.body.pizza,
      "accepted"
    ];
  const result = await client.execute(query, orderDetail, 
    {
      prepare: true
    })
  producer.send({
    data: Buffer.from(JSON.stringify(orderDetail)),
  });
  res.status(201).json({"status": "created"})
  await producer.flush();
})

/* Start Server*/
const server = app.listen(port, async () => {
  console.log(`Example app listening on port ${port}`)
  const consumer = await pulsarClient.subscribe({
    topic: topicName,
    subscription: subscriptionName,
    subscriptionType: "Exclusive",
    ackTimeoutMs: 10000,
  });
  // Receive and acknowledge messages
  for (let i = 0; i < 100; i += 1) {
    const msg = await consumer.receive();
    if(msg.getData().toString().toLowerCase().indexOf("delivery") > -1) {
      console.log("Trigger Driver SMS for :" + msg.getData().toString());
    }
    consumer.acknowledge(msg);
  }
  await consumer.close();
})

/* Graceful shutdown process */
process.on('SIGTERM', async () => {
  await server.close()
  await client.shutdown()
  await producer.close();
  await pulsarClient.close();
});
