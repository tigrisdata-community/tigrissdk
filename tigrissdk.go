// Package tigrissdk contains a Tigris client and helpers for interacting with Tigris.
//
// Tigris is a cloud storage service that provides a simple, scalable, and secure object storage solution. It is based on the S3 API, but has additional features that need these helpers.
package tigrissdk

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Options struct {
	BaseEndpoint string
	Region       string
	UsePathStyle bool

	AccessKeyID     string
	SecretAccessKey string
}

func (Options) Default() Options {
	return Options{
		BaseEndpoint: "https://t3.storage.dev",
		Region:       "auto",
		UsePathStyle: false,
	}
}

type Option func(o *Options)

func WithFlyEndpoint() Option {
	return func(o *Options) {
		o.BaseEndpoint = "https://fly.storage.tigris.dev"
	}
}

func WithGlobalEndpoint() Option {
	return func(o *Options) {
		o.BaseEndpoint = "https://t3.storage.dev"
	}
}

func WithRegion(region string) Option {
	return func(o *Options) {
		o.Region = region
	}
}

func WithAccessKeypair(accessKeyID, secretAccessKey string) Option {
	return func(o *Options) {
		o.AccessKeyID = accessKeyID
		o.SecretAccessKey = secretAccessKey
	}
}

// Client returns a new S3 client wired up for Tigris.
func New(ctx context.Context, options ...Option) (*Client, error) {
	o := new(Options).Default()

	for _, doer := range options {
		doer(&o)
	}

	var creds aws.CredentialsProvider

	if o.AccessKeyID != "" && o.SecretAccessKey != "" {
		creds = credentials.NewStaticCredentialsProvider(o.AccessKeyID, o.SecretAccessKey, "")
	}

	cfg, err := awsConfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load Tigris config: %w", err)
	}

	cli := s3.NewFromConfig(cfg, func(opts *s3.Options) {
		opts.BaseEndpoint = aws.String(o.BaseEndpoint)
		opts.Region = o.Region
		opts.UsePathStyle = o.UsePathStyle
		opts.Credentials = creds
	})

	return &Client{
		Client: cli,
	}, nil
}
