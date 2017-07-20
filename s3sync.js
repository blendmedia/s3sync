var AWS = require('aws-sdk'),
    async = require('async'),
    config = require('./config'),
    fetch = require('node-fetch'),
    _  = require('lodash'),
    logging = require('./logging'),
    moment = require('moment'),
    sqs = require('sqs-consumer'),
    logger = logging.getLogger('s3sync');

AWS.config.update(config.aws);

var buckets = config.s3sync.buckets;

function setupS3Clients() {
  clients = {};

  Object.keys(config.credentials).map(function(region, idx) {
    clients[region] = new AWS.S3(
      config.credentials[region]
    );
  });

  return clients;
}

async function handleMessage(message, done) {
  var params = {
    QueueUrl: config.s3sync.sqs.url,
    ReceiptHandle: message.ReceiptHandle
  };

  sqsClient.deleteMessage(params, function(err, data) {
    if (err) console.log("Was unable to delete the message from SQS");
    else     console.log("Message deleted from SQS.");
  });

  const body = JSON.parse(message.Body);
  console.log("Handling SQS message: ");
  console.dir(body);

  const { job_id: jobId, files } = body;

  var remainingFiles = Array.from(files);

  try {
    for (const file of files) {
      src = config.s3sync.regions.filter(function(region) {
        return region.region == buckets[0].srcregion;
      })[0].suffix;

      srcS3 = s3[src];

      dest = config.s3sync.regions.filter(function(region) {
        return region.region == buckets[0].destregions[0];
      })[0].suffix;

      destS3 = s3[dest];

      const params = {
        Bucket: buckets[0].dest,
        Key: file,
        Body: srcS3.getObject({
          Bucket: buckets[0].src,
          Key: file,
        }).createReadStream(),
      };

      const upload = destS3.upload(params);

      console.log("Uploading " + file + "...");

      upload.on("httpUploadProgress", async evt => {
        await fetch("http://localhost:8080/api/sync_jobs/" + jobId, {
          method: "PUT",
          headers: {
            "content-type": "application/json",
            "authorization": `Bearer ${config.s3sync.apiToken}`
          },
          body: JSON.stringify({
            completed: false,
          }),
        });

        console.log('Progress:', evt.loaded, '/', evt.total);
      });

      await upload.promise();
      await fetch("http://localhost:8080/api/sync_jobs/" + jobId, {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          "authorization": `Bearer ${config.s3sync.apiToken}`
        },
        body: JSON.stringify({
          completed: false,
        }),
      });

      var index = remainingFiles.indexOf(file)
      remainingFiles.splice(index, 1);

      console.log("Completed upload of " + file + ".");
      console.log("Files remaining:")
      console.dir(remainingFiles);
    }

    await fetch("http://localhost:8080/api/sync_jobs/" + jobId, {
      method: "PUT", headers: {
        "content-type": "application/json",
        "authorization": `Bearer ${config.s3sync.apiToken}`
      },
      body: JSON.stringify({
        completed: true,
      }),
    });

    console.log("Uploaded all files.");
    done();
  } catch(e) {
    console.log("Files left before failure:")
    console.dir(remainingFiles);

    sqsClient.sendMessage({
      QueueUrl: config.s3sync.sqs.url,
      MessageBody: JSON.stringify(
        Object.assign(
          {},
          body,
          { files: remainingFiles }
        )
      )
    }, function(err, data) {
      if (err) console.log("Was unable to recreate the message from SQS");
      else     console.log("Message created again on SQS.");
    });

    return done(e);
  }
}

function sqsSync(cb) {
    logger.info("Connecting to SQS queue at " + config.s3sync.sqs.url);
    var q = sqs.create({
        queueUrl: config.s3sync.sqs.url,
        region: config.s3sync.sqs.region,
        handleMessage: handleMessage,
        batchSize:10
    });
    q.start();
    q.on("error", function(er) {
        logger.error("caught " + er);
    });
    function stop() {
        logger.info("Shutting down SQS. May take up to 30 seconds.")
        q.stop();
        cb();
    }
    shutdownfuncs.push(stop);
}

function noop() {}

var shutdownfuncs = [];
var s3 = setupS3Clients();
var sqsClient = new AWS.SQS(config.credentials.euw1);

function shutdown() {
    logger.warn("Caught shutdown signal.  Finishing jobs in process (send SIGTERM to forcefully kill)");
    shutdownfuncs.forEach(function(f){f()});
    shutdownfuncs = [];
}

if (config.s3sync.sqs) {
    sqsSync(noop);
}

if (shutdownfuncs.length > 0) {
    process.on('SIGINT', shutdown);
    process.on('SIGHUP', shutdown);
}

process.on('uncaughtException', function(err) {
    logger.error("Uncaught exception.  Shutting down!");
    logger.error(err);
    // Use setImmediate to force a clear callstach & allow shutdown() to finish processing shutdownfuncs at least once
    setImmediate(function() {
        shutdown();
        setTimeout(function(){process.exit(-1)}, 30000).unref();
    });
});
