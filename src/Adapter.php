<?php

namespace TractorCow\AWSStore;

use Aws\S3\S3Client;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use LogicException;

/**
 * An adapter that allows the use of AWS S3 to store and transmit assets rather than storing them locally.
 */
class Adapter extends AwsS3Adapter implements CombinedAdapter
{

    public function __construct(S3Client $s3Client)
    {
        parent::__construct($s3Client, $this->findAwsBucket(), $this->findBucketPrefix());
    }

    /**
     * Used by SilverStripe to get the protected URL for this file. This utilises the default ProtectedFileController
     * class to read the file from AWS (rather than linking to AWS directly).
     *
     * @param string $path
     * @return string
     */
    public function getProtectedUrl($path)
    {
        $cmd = $this->getClient()
            ->getCommand('GetObject', [
                'Bucket' => $this->getBucket(),
                'Key' => $this->applyPathPrefix($path)
            ]);

        $request = $this->getClient()->createPresignedRequest($cmd, '+15 minutes');

        // Get the actual presigned-url
        return (string)$request->getUri();
    }

    /**
     * Used to get the public URL to a file in an S3 bucket. The standard S3 URL is returned (the file is not proxied
     * further via SilverStripe).
     *
     * @param string $path
     * @return string
     */
    public function getPublicUrl($path)
    {
        return $this
            ->getClient()
            ->getObjectUrl($this->getBucket(), $this->applyPathPrefix($path));
    }

    /**
     * @return string
     * @throws LogicException
     */
    public function findAwsBucket()
    {
        if (getenv('AWS_BUCKET_NAME') !== false) {
            return (string)getenv('AWS_BUCKET_NAME');
        }
        throw new LogicException(
            'No valid AWS S3 bucket found. Please set AWS_BUCKET_NAME to your env.'
        );
    }


    /**
     * @return string
     */
    public function findBucketPrefix()
    {
        return getenv('AWS_BUCKET_PREFIX') ?: '';
    }
}
