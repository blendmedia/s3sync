const rc = require("rc");

const defaults = {
  aws: {
    maxRetries: 10,
    retryDelayOptions: {
      base: 1000
    }
  },

  credentials: {
    euw1: {
      accessKeyId: "--REDACTED--",
      region: "eu-west-1",
      secretAccessKey: "--REDACTED--"
    },
    cnn1: {
      accessKeyId: "--REDACTED--",
      region: "cn-north-1",
      secretAccessKey: "--REDACTED--"
    }
  },

  log4js: {
    level: "INFO",
    replaceConsole: true,
    appenders: [
      {
        type: "console",
      },
    ],
  },

  s3sync: {
    apiToken: "--REDACTED--",
    buckets: [
      {
        src: "--REDACTED--",
        srcregion: "eu-west-1",
        dest: "--REDACTED--",
        destregions: ["cn-north-1"]
      }
    ],
    regions: [
      {
        region: "cn-north-1",
        suffix: "cnn1"
      },
      {
        region: "eu-west-1",
        suffix: "euw1"
      }
    ],
    sqs: {
      url: "--REDACTED--",
      region: "eu-west-1",
    },
  },
};

module.exports = rc("s3sync", defaults);
