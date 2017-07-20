const rc = require("rc");

const defaults = {
  aws: {
    accessKeyId: "",
    secretAccessKey: "",
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
    regions: [
      {
        region: "eu-west-1",
        suffix: ".euw1",
      },
    ],
    buckets: [
      {
        src: "blend-content",
        srcregion: "eu-west-1",
        dest: "blend-chris-sync-test",
        destregions: ["eu-west-1"],
      },
    ],
    sqs: {
      url: "https://sqs.eu-west-1.amazonaws.com/480869150897/chris-sync-test",
      region: "eu-west-1",
    },
  },
};

module.exports = rc("s3sync", defaults);
