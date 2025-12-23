package tigrissdk

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tigrisdata-community/tigrissdk/tigrisheaders"
)

type Client struct {
	*s3.Client
}

type CreateBucketSnapshotInput struct {
	s3.CreateBucketInput

	Description string
}

func (c *Client) CreateBucketFork(ctx context.Context, source, name string, opts ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	opts = append(opts, tigrisheaders.WithHeader("X-Tigris-Fork-Source-Bucket", source))

	return c.Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	}, opts...)
}

func (c *Client) CreateBucketSnapshot(ctx context.Context, in *CreateBucketSnapshotInput, opts ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	opts = append(opts, tigrisheaders.WithTakeSnapshot(in.Description))

	return c.Client.CreateBucket(ctx, &in.CreateBucketInput, opts...)
}

func (c *Client) CreateSnapshottableBucket(ctx context.Context, in *s3.CreateBucketInput, opts ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	opts = append(opts, tigrisheaders.WithEnableSnapshot())

	return c.Client.CreateBucket(ctx, in, opts...)
}

type HeadBucketForkOrSnapshotOutput struct {
	SnapshotsEnabled     bool
	SourceBucket         string
	SourceBucketSnapshot string
	IsForkParent         bool
}

func (c *Client) HeadBucketForkOrSnapshot(ctx context.Context, in *s3.HeadBucketInput, opts ...func(*s3.Options)) (*HeadBucketForkOrSnapshotOutput, error) {
	resp, err := c.Client.HeadBucket(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	rawResp := middleware.GetRawResponse(resp.ResultMetadata).(*http.Response)
	return &HeadBucketForkOrSnapshotOutput{
		SnapshotsEnabled:     rawResp.Header.Get("X-Tigris-Enable-Snapshot") == "true",
		SourceBucket:         rawResp.Header.Get("X-Tigris-Fork-Source-Bucket"),
		SourceBucketSnapshot: rawResp.Header.Get("X-Tigris-Fork-Source-Bucket-Snapshot"),
		IsForkParent:         rawResp.Header.Get("X-Tigris-Is-Fork-Parent") == "true",
	}, nil
}

func (c *Client) ListBucketSnapshots(ctx context.Context, bucketName string, opts ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	opts = append(opts, tigrisheaders.WithHeader("X-Tigris-Snapshot", bucketName))

	return c.Client.ListBuckets(ctx, &s3.ListBucketsInput{}, opts...)
}

func (c *Client) RenameObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	opts = append(opts, tigrisheaders.WithRename())

	return c.Client.CopyObject(ctx, in, opts...)
}
