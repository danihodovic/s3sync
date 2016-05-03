## Faster S3 sync

s3cmd is slow, aws-cli can't handle prefixes, we can do better.

### Usage
```bash
$ s3sync --url s3://my-bucket/my-prefix --output dest-folder
```

If you want to use a specific profile defined in your `~/.aws/credentials` you can set an
environment variable like so:

```bash
$ AWS_PROFILE=test s3sync --url s3://my-bucket/my-prefix --output dest-folder
```
