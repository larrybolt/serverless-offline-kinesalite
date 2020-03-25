import { join } from "path";
import Kinesis from "aws-sdk/clients/kinesis";

const functionHelper = require("serverless-offline/src/functionHelper");
const LambdaContext = require("serverless-offline/src/LambdaContext");
const kinesalite = require("kinesalite");
const kinesaliteServer = kinesalite({ createStreamMs: 0 });

import { StreamListener } from "./streamListener";
import Serverless, { Options } from "serverless";
import Service from "serverless/classes/Service";
import { KinesisRecordListToLambdaEvent } from "./transformers";

export interface StringEventObject {
  type: "kinesis";
  streamName?: string;
  batchSize?: number;
  batchWindow?: number;
  startingPosition: Kinesis.ShardIteratorType; //'LATEST' | 'TRIM_HORIZON';
  maximumRetryAttempts?: 1;
  bisectBatchOnFunctionError?: boolean;
  arn?:
    | string
    | {
        "Fn::GetAtt": string[];
      };
}
export type StreamEvent = string | StringEventObject;

const printBlankLine = () => console.log();

const extractStreamNameFromARN = (arn: string) => {
  const [, , , , , StreamURI] = arn.split(":");
  const [, ...StreamNames] = StreamURI.split("/");
  return StreamNames.join("/");
};

type ServiceWithResource = Service & {
  provider: Service["provider"] & {
    environment?: any;
  };
  functions?: {
    [functionName: string]: {
      handler: string;
      timeout?: number;
      events: {
        stream: StringEventObject;
        [eventType: string]: {};
      }[];
      name: string;
    };
  };
  resources?: {
    Resources?: {
      [ResourceName: string]: {
        Properties?: {
          Name: string;
          ShardCount: number;
        };
      };
    };
  };
};

export class ServerlessOfflinekinesalite {
  serverless: Serverless;
  service: ServiceWithResource;
  options: Options;
  commands = {};
  hooks: { [key: string]: () => void };
  streams = [];

  constructor(serverless: Serverless, options: Options) {
    this.serverless = serverless;
    this.service = serverless.service;
    this.options = options;
    this.hooks = {
      "before:offline:start": this.offlineStartInit.bind(this),
      "before:offline:start:init": this.offlineStartInit.bind(this),
      "before:offline:start:end": this.offlineStartEnd.bind(this),
    };
  }

  getConfig() {
    return {
      ...this.options,
      ...this.service,
      ...this.service.provider,
      ...this.service?.custom["serverless-offline"],
      ...this.service?.custom["serverless-offline-kinesalite"],
    };
  }

  getClient() {
    return new Kinesis(this.getConfig());
  }

  eventHandler = (
    streamEvent: StreamEvent,
    functionName: string,
    shardId: string,
    chunk: Kinesis.RecordList,
    retryAttempt: number
  ) =>
    new Promise((resolve, reject) => {
      const streamName = this.getStreamName(streamEvent);
      printBlankLine();
      this.serverless.cli.log(
        `kinesis stream/${streamName} (λ: ${functionName})`
      );

      const location = this.getConfig().location ?? ".";

      const __function = this.service.getFunction(functionName);

      const oldEnv = { ...process.env };
      const functionEnv = {
        AWS_REGION: this.service.provider.region,
        ...process.env,
        ...this.service.provider.environment,
        ...__function.environment,
      };
      process.env = functionEnv;

      const serviceRuntime = this.service.provider.runtime;
      const servicePath = join(this.serverless.config.servicePath, location);
      const funOptions = functionHelper.getFunctionOptions(
        __function,
        functionName,
        servicePath,
        serviceRuntime
      );
      const handler = functionHelper.createHandler(
        funOptions,
        this.getConfig()
      );
      const lambdaContext = new LambdaContext(
        __function,
        this.service.provider,
        (err: any, data: any) => {
          this.serverless.cli.log(
            `[${err ? "✖" : "✔"}] ${
              retryAttempt === 0 ? "" : `(retry: ${retryAttempt}) `
            }${functionName} ${JSON.stringify(data) || ""}`
          );
          if (err) {
            reject(err);
            return;
          }
          resolve(data);
        }
      );

      const awsRegion = this.service.provider.region;
      const event = KinesisRecordListToLambdaEvent(
        awsRegion,
        streamName,
        shardId,
        chunk
      );

      let check = false;
      const x = handler(event, lambdaContext, (error: any, response: any) => {
        check = true;
        if (error) {
          lambdaContext.fail(error);
        } else {
          lambdaContext.succeed(response);
        }
      });
      if (
        !check &&
        x &&
        typeof x.then === "function" &&
        typeof x.catch === "function"
      )
        x.then((r: any) => {
          lambdaContext.succeed(r);
        }).catch((e: any) => {
          lambdaContext.fail(e);
        });
      else if (x instanceof Error) lambdaContext.fail(x);

      process.env = oldEnv;
    });

  getStreamProperties(streamEvent: StreamEvent) {
    if (
      typeof streamEvent !== "string" &&
      typeof streamEvent.arn === "object" &&
      streamEvent.arn["Fn::GetAtt"]
    ) {
      const [ResourceName] = streamEvent.arn["Fn::GetAtt"];
      if (
        this.service.resources?.Resources &&
        this.service.resources.Resources[ResourceName].Properties
      )
        return this.service.resources.Resources[ResourceName].Properties;
    }
    throw new Error("Properties not found!");
  }

  getStreamName(streamEvent: StreamEvent) {
    if (typeof streamEvent === "string") {
      if (streamEvent.indexOf("arn:aws:kinesis") === 0)
        return extractStreamNameFromARN(streamEvent);
      else
        throw new Error(
          `StreamEvent '${streamEvent}' doesn't begin with arn:aws:kinesis`
        );
    }
    if (typeof streamEvent.arn === "string")
      return extractStreamNameFromARN(streamEvent.arn);
    if (typeof streamEvent.streamName === "string")
      return streamEvent.streamName;
    const properties = this.getStreamProperties(streamEvent);
    if (properties?.Name) {
      return properties.Name;
    }
    throw new Error(`StreamName not found.`);
  }

  getStreamConfig(streamEvent: StreamEvent) {
    if (typeof streamEvent === "string") {
      return {
        batchSize: 1,
        batchWindow: 1,
      };
    }
    return streamEvent;
  }

  async createKinesisReadable(functionName: string, streamEvent: StreamEvent) {
    const client = this.getClient();
    const StreamName = this.getStreamName(streamEvent);

    this.serverless.cli.log(`streamName: ${StreamName}`);
    const properties = this.getStreamProperties(streamEvent);
    const ShardCount = properties?.ShardCount || 1;
    try {
      await client.createStream({ ShardCount, StreamName }).promise();
      console.info(
        `Kinesis stream '${StreamName}' created with ${ShardCount} shards`
      );
    } catch (e) {
      if (e.code === "ResourceInUseException") {
        console.info("Kinesis stream already exists");
      }
    }

    const {
      StreamDescription: { Shards: shards },
    } = await client.describeStream({ StreamName }).promise();

    await Promise.all(
      shards.map(async ({ ShardId: shardId }) => {
        await StreamListener(
          client,
          StreamName,
          shardId,
          this.getStreamConfig(streamEvent),
          async (batch, attempt) =>
            this.eventHandler(
              streamEvent,
              functionName,
              shardId,
              batch,
              attempt
            )
        );
      })
    );
  }

  offlineStartInit() {
    this.serverless.cli.log(`Starting Offline Kinesis.`);
    const { endpoint } = this.getConfig();
    const port = parseInt(endpoint.split(":")[2]);
    kinesaliteServer.listen(port, (err: any) => {
      if (err) throw err;
      this.serverless.cli.log(`Kinesalite started on port ${port}`);
      if (!this.service.functions) return;
      for (const functionName in this.service.functions) {
        const _function = this.service.functions[functionName];
        for (const event of _function.events) {
          if (!event.stream) continue;
          this.createKinesisReadable(functionName, event.stream).then();
        }
      }
    });
  }
  offlineStartEnd() {}
}
module.exports = ServerlessOfflinekinesalite;
