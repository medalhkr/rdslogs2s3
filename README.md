rdslogs2s3
===

Save RDS Logs file to S3.

## Require
- python3
- boto3

## IAM Role

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::[BucketName]/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::[BucketName]"
            ]
        },
        {

            "Effect": "Allow",
            "Action": [
                "rds:DescribeDBLogFiles",
                "rds:DownloadDBLogFilePortion"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
```
