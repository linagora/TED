import * as client from "prom-client";

export function setup():void
{
    client.collectDefaultMetrics({
        gcDurationBuckets: [0.001, 0.01, 0.1, 1, 2, 5], // These are the default buckets.
    });
}
