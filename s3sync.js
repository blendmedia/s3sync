const AWS           = require("aws-sdk");
const config        = require("./config");
const fetch         = require("node-fetch");
const sqs           = require("sqs-consumer");
const logging       = require("./logging");
const logger        = logging.getLogger("s3sync");

AWS.config.update(config.aws);

const { buckets, regions } = config.s3sync;

function request(url, options) {
  const opts = Object.assign({}, options, {
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${config.s3sync.apiToken}`,
    },
  });

  if ("data" in options) {
    options.body = JSON.stringify(options.data);
  }

  return fetch(`http://localhost:8080/api/${url}`, opts);
}

function setupS3Clients() {
  return Object.entries(config.credentials)
    .reduce((o, [region, credentials]) => {
      o[region] = new AWS.S3(
        credentials
      );
      return o;
    }, {});
}

let processing = null, interruptedFiles = [], shuttingDown = false;
async function handleMessage(message, done) {
  const params = {
    QueueUrl: config.s3sync.sqs.url,
    ReceiptHandle: message.ReceiptHandle,
  };

  try {
    await sqsClient.deleteMessage(params).promise();
    console.log("Message deleted from SQS.");
  } catch (e) {
    console.log("Was unable to delete the message from SQS");
  }

  const body = JSON.parse(message.Body);
  processing = body;
  const { job_id: jobId, files } = body;
  console.log("Handling SQS message:", body);

  let remainingFiles = interruptedFiles = files;
  const processedFiles = [];
  try {
    for (const file of files) {
      const src = regions.find(r => (
        r.region === buckets[0].srcregion
      )).suffix;
      const srcS3 = s3[src];

      const dest = regions.find(r => (
        r.region === buckets[0].destregions[0]
      )).suffix;
      const destS3 = s3[dest];

      const params = {
        Bucket: buckets[0].dest,
        Key: file,
        Body: srcS3.getObject({
          Bucket: buckets[0].src,
          Key: file,
        }).createReadStream(),
      };
      const upload = destS3.upload(params);

      console.log(`Uploading ${file}...`);
      upload.on("httpUploadProgress", async evt => {
        await request(`sync_jobs/${jobId}`, {
          method: "PUT",
          data: {
            completed: false,
          },
        });

        console.log("Progress:", evt.loaded, "/", evt.total);
      });

      await upload.promise();
      await request(`sync_jobs/${jobId}`, {
        method: "PUT",
        data: {
          completed: false,
        },
      });

      remainingFiles = interruptedFiles = remainingFiles.filter(
        f => f !== file
      );
      processedFiles.push(file);

      console.log(`Completed upload of ${file}.`);
      if (shuttingDown) break;
      else {
        console.log("Files remaining:");
        console.dir(remainingFiles);
      }

    }

    if (!shuttingDown) {
      await request(`sync_jobs/${jobId}`, {
        method: "PUT",
        data: {
          completed: true,
        },
      });

      console.log("Uploaded all files.");
      done();
    }
  } catch(e) {
    console.log("Files left before failure:");
    console.dir(remainingFiles);

    sqsClient.sendMessage({
      QueueUrl: config.s3sync.sqs.url,
      MessageBody: JSON.stringify(
        Object.assign(
          {},
          body,
          { files: remainingFiles }
        )
      ),
    }, function(err) {
      if (err) {
        console.log("Was unable to recreate the message from SQS");
      } else     {
        console.log("Message created again on SQS.");
      }
    });

    return done(e);
  } finally {
    if (!shuttingDown) {
      processing = null;
    }
  }
}

function sqsSync() {
  logger.info(`Connecting to SQS queue at ${config.s3sync.sqs.url}`);
  const q = sqs.create({
    queueUrl: config.s3sync.sqs.url,
    region: config.s3sync.sqs.region,
    handleMessage,
    batchSize: 10,
  });
  q.start();
  q.on("error", function(er) {
    logger.error(`caught ${er}`);
  });
  function stop() {
    logger.info("Shutting down SQS. May take up to 30 seconds.");
    q.stop();
  }
  shutdownfuncs.push(stop);
}


let shutdownfuncs = [];
const s3 = setupS3Clients();
const sqsClient = new AWS.SQS(config.credentials.euw1);

function shutdown() {
  if (!shuttingDown) {
    shuttingDown = true;
    logger.warn([
      "Caught shutdown signal.",
      "Finishing jobs in process (send SIGTERM to forcefully kill)",
    ].join(" "));
    if (processing) {
      // This contains the original message object
      console.log("Files left before interruption:");
      console.dir(interruptedFiles);

      sqsClient.sendMessage({
        QueueUrl: config.s3sync.sqs.url,
        MessageBody: JSON.stringify(
          Object.assign(
            {},
            processing,
            { files: interruptedFiles }
          )
        ),
      }, function(err) {
        if (err) {
          console.log("Was unable to recreate the message from SQS");
        } else     {
          console.log("Message created again on SQS.");
        }
      });
    }
    for (const f of shutdownfuncs) {
      f();
    }
    shutdownfuncs = [];
  }
}

sqsSync();

if (shutdownfuncs.length > 0) {
  process.on("SIGINT", shutdown);
  process.on("SIGHUP", shutdown);
}

process.on("uncaughtException", function(err) {
  logger.error("Uncaught exception.  Shutting down!");
  logger.error(err);
  // Use setImmediate to force a clear callstach & allow shutdown()
  // to finish processing shutdownfuncs at least once
  setImmediate(function() {
    shutdown();
    setTimeout(function(){
      process.exit(-1);
    }, 30000).unref();
  });
});
