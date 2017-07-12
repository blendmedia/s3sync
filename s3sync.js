var AWS = require('aws-sdk'),
    async = require('async'),
    config = require('./config'),
    _  = require('lodash'),
    logging = require('./logging'),
    moment = require('moment'),
    sqs = require('sqs-consumer'),
    logger = logging.getLogger('s3sync');

AWS.config.update(config.aws);

var MAX_COPY_SIZE = 5 * 1024 * 1024 * 1024; // 5GB

var getAWSLogger = _.memoize(function (category) {
    var logger = logging.getLogger(category);
    return {
      log: function(o) { logger.trace(o) }
    }
});

var commonParams = {
    ServerSideEncryption: "AES256",
    StorageClass: "REDUCED_REDUNDANCY"
};

var regions = config.s3sync.regions,
    buckets = config.s3sync.buckets;

function handleMessage(message, done) {
  /* Message format:
   *
   * { job: 'sync',
   *   id: 123,
   *   files: [
   *     'aws-prefix/file.mp4',
   *     'aws-prefix/file.png'
   *   ]
   * }
   *
   */
  try {
    body = JSON.parse(message.Body);
  } catch(e) {
    return done(e);
  }

  console.log("Handling SQS message: ")
  console.dir(body)

  var s3 = new AWS.S3();

  promises = body.files.map(function(file) {
    var params = {
      Bucket: buckets[0].dest,
      CopySource: buckets[0].src + "/" + file,
      Key: file
    };

    var putObjectPromise = s3.copyObject(params).promise();
    putObjectPromise.then(function(data) {
      return data;
    }).catch(function(err) {
      done(err);
    });
  });

  Promise.all(promises).then(
    function() {
      console.log("Copied all files successfully.");
      done();
    }
  );
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
