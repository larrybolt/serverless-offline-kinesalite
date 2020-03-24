import { Kinesis } from "aws-sdk";

export function KinesisRecordListToLambdaEvent(
  awsRegion: string,
  StreamName: string,
  ShardId: string,
  Records: Kinesis.RecordList
): AWSLambda.KinesisStreamEvent {
  return {
    Records: Records.map((record) => ({
      kinesis: {
        kinesisSchemaVersion: "1.0",
        partitionKey: record.PartitionKey,
        sequenceNumber: record.SequenceNumber,
        data: Buffer.from(record.Data).toString("base64"),
        // data: Data.toString("base64"),
        approximateArrivalTimestamp: record.ApproximateArrivalTimestamp!.getTime(), // TODO: check if this is currect
      },
      eventSource: "aws:kinesis",
      eventVersion: "1.0",
      eventID: `${ShardId}:${record.SequenceNumber}`,
      eventName: "aws:kinesis:record",
      invokeIdentityArn: "arn:aws:iam::serverless:role/offline", // "arn:aws:iam::045797332296:role/lry-test-sls-dev-eu-west-1-lambdaRole",
      awsRegion,
      eventSourceARN: `arn:aws:kinesis:${awsRegion}:045797332296:stream/${StreamName}`, // TODO: fix this
    })),
  };
}
