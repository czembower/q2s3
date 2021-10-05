# q2s3

Utility to synchronize data from a Qumulo cluster running in AWS to an S3 bucket.

Should be run on all of the cluster nodes as a nightly cron job to facilitate content backups. Data movement is distributed in a fashion such that all of the nodes participate in the transfer in parallel, and a basic algorithm controls which nodes are responsible for copying individual files.
