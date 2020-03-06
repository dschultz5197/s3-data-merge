class S3Object:
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key
        self.path = 's3://{}/{}'.format(bucket, key)


