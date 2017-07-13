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

  console.log("Handling SQS message: ");
  console.dir(body);

  var s3 = new AWS.S3();
  var jobId = body.job_id;

  promises = Promise.all(
    body.files.map(function(file) {
      var params = {
        Bucket: buckets[0].dest,
        Key: file,
        Body: s3.getObject({Bucket: buckets[0].src, Key: file}).createReadStream(),
      };

        upload = s3.upload(params);
        upload.on(
          'httpUploadProgress', function(evt) {
            return fetch("http://localhost:8080/api/sync_jobs/" + jobId, {
              method: "PUT",
              body: "{\"completed\": false}"
            }).then(function(res) {
              console.log('Progress:', evt.loaded, '/', evt.total);
            });
          }
        );
      return upload.promise().
        then(function() {
          return fetch("http://localhost:8080/api/sync_jobs/" + jobId, {
            method: "PUT",
            body: "{\"completed\": false}"
          }).then(function(res) {
            console.log("Done one file.")
          });
        }).catch(function(err) {
          done(err);
        });
    }));

  promises.
    then(function() {
      return fetch("http://localhost:8080/api/sync_jobs/" + jobId, {
        method: "PUT",
        body: "{\"completed\": true}"
      }).then(function(res) {
        console.log("Done.");
        done();
      });
    }).catch(function(err) {
      done(err);
    });
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
