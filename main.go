package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("sa-east-1"))
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(os.Getenv("S3_URL"))
		o.UsePathStyle = true
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationUnset
	})

	bucketsOutput, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		log.Fatal(err)
	}

	for _, bucket := range bucketsOutput.Buckets {
		log.Printf("Bucket: %s \t\t Created at: %v\n\n", *bucket.Name, bucket.CreationDate.Local())
	}

	var output *s3.ListObjectsV2Output
	var objects []types.Object

	objectPaginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(os.Getenv("S3_BUCKET")),
	})

	for {
		for objectPaginator.HasMorePages() {
			output, err = objectPaginator.NextPage(context.TODO())
			if err != nil {
				var noBucket *types.NoSuchBucket
				if errors.As(err, &noBucket) {
					log.Printf("Bucket %s does not exist.\n", os.Getenv("S3_BUCKET"))
					err = noBucket
				}
				break
			} else {
				objects = append(objects, output.Contents...)
			}
		}

		var downloadedFiles = make(map[string]bool)

		for _, object := range objects {
			if !downloadedFiles[*object.Key] {
				downloadedFiles[*object.Key] = true

				fmt.Printf("Downloading new file: %s\n", *object.Key)
				err := downloadFile(client, *object.Key)
				if err != nil {
					log.Printf("Error downloading file %s: %v", *object.Key, err)
				}
			}
		}

		objects = nil

		time.Sleep(10 * time.Second)
	}
}

func downloadFile(client *s3.Client, key string) error {
	if getParentDir(key) != "" {
		if err := os.MkdirAll(getParentDir(key), os.ModePerm); err != nil {
			return fmt.Errorf("failed to create directories for %s: %w", key, err)
		}
	}

	file, err := os.Create(key)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	downloader := manager.NewDownloader(client)
	_, err = downloader.Download(context.TODO(), file, &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("S3_BUCKET")),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file %s: %w", key, err)
	}

	fmt.Printf("Download succeeded.\n\n")

	return nil
}

func getParentDir(key string) string {
	lastSlash := len(key) - len(key[strings.LastIndex(key, "/")+1:])
	if lastSlash > 0 {
		return key[:lastSlash]
	}
	return ""
}
