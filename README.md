# serverless-offline-kinesalite

This Serverless-offline plugin is the marriage between [serverless-offline-kinesis](https://www.npmjs.com/package/serverless-offline-kinesis) and [kinesalite](https://github.com/mhart/kinesalite).

*Features*:
- Kinesis configurations: batchSize, batchWindow, maximumRetryAttempts, bisectBatchOnFunctionError and startingPosition.

## Installation

First, add `serverless-offline-kinesalite` to your project:

```sh
npm install --save-dev serverless-offline-kinesalite
```

Then inside your project's `serverless.yml` file, add following entry to the plugins section before `serverless-offline`: `serverless-offline-kinesalite`.

```yml
plugins:
  - serverless-offline-kinesalite
  - serverless-offline
```
