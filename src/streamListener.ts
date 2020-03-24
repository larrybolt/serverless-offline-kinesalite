import { Kinesis } from "aws-sdk";
import { promisify } from "util";
import Queue from "better-queue";

const timeout = promisify(setTimeout);

interface KinesisStreamListenerConfig {
  batchSize: number;
  batchWindow: number;
  startingPosition: Kinesis.ShardIteratorType; //'LATEST' | 'TRIM_HORIZON';
  maximumRetryAttempts: number;
  bisectBatchOnFunctionError: boolean;
  parallelizationFactor: number;
}
const defaults: KinesisStreamListenerConfig = {
  batchSize: 1,
  batchWindow: 0,
  startingPosition: "LATEST",
  maximumRetryAttempts: 0,
  bisectBatchOnFunctionError: false,
  parallelizationFactor: 0,
};

export async function StreamListener(
  client: Kinesis,
  StreamName: string,
  ShardId: string,
  _options: Partial<KinesisStreamListenerConfig>,
  handler: (batch: Kinesis.RecordList, attempt: number) => Promise<unknown>
): Promise<void> {
  const options: KinesisStreamListenerConfig = {
    ...defaults,
    ..._options,
  };

  const params: Kinesis.Types.GetShardIteratorInput = {
    ShardId,
    ShardIteratorType: options.startingPosition,
    StreamName,
    // StartingSequenceNumber: 'STRING_VALUE',
    // Timestamp: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789
  };
  const { ShardIterator } = await client.getShardIterator(params).promise();
  if (!ShardIterator) {
    return;
  }
  const queue = new Queue(
    async (batch: Kinesis.RecordList, cb) => {
      const { maximumRetryAttempts, bisectBatchOnFunctionError } = options;
      async function handleAttempt(batch: Kinesis.RecordList, attempt: number) {
        try {
          await handler(batch, attempt);
          cb(null);
        } catch (e) {
          if (attempt >= maximumRetryAttempts) {
            cb(null);
            return;
          }

          if (bisectBatchOnFunctionError && batch.length > 1) {
            const chunkA = batch.slice(0, Math.ceil(batch.length / 2));
            const chunkB = batch.slice(Math.ceil(batch.length / 2));
            await handleAttempt(chunkA, 0);
            await handleAttempt(chunkB, 0);
            return;
          }
          if (attempt < maximumRetryAttempts) {
            await handleAttempt(batch, attempt + 1);
          }
        }
      }
      await handleAttempt(batch, 0);
    },
    {
      batchSize: options.batchSize,
      batchDelayTimeout: options.batchWindow * 1000,
      batchDelay: 100,
    }
  );
  const Limit = options.batchSize;
  async function readStream(ShardIterator: string) {
    const { Records, NextShardIterator } = await client
      .getRecords({ ShardIterator, Limit })
      .promise();
    if (!NextShardIterator) {
      throw new Error("No ShardIterator received");
    }
    Records.forEach((record) => queue.push(record));
    await timeout(200);
    await readStream(NextShardIterator);
  }
  await readStream(ShardIterator);
}
