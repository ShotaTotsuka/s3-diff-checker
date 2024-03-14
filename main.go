package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
)

func init() {
	if err := godotenv.Load(); err != nil {
		panic(err)
	}
}

func main() {
	const s3DirPrefix = "tarot/"

	sdkConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	s3Client := s3.NewFromConfig(sdkConfig)

	bucketBasics := BucketBasics{
		S3Client: s3Client,
	}

	bucketName := os.Getenv("S3_BUCKET_NAME")

	s3ObjMap := map[string]bool{}

	// S3のバケットから全てのファイル名を取得する
	s3Objects, err := bucketBasics.ListObjects(bucketName)
	if err != nil {
		panic(err)
	}
	for _, s3Object := range s3Objects {
		if *s3Object.Key == s3DirPrefix {
			continue
		}
		s3ObjMap[*s3Object.Key] = false
	}

	// ./imagesから全てのファイル名を取得する
	localDir, err := os.ReadDir("./images")
	if err != nil {
		panic(err)
	}

	// s3ObjMapにあるファイルはtrueに変更する
	for _, localFile := range localDir {
		if _, ok := s3ObjMap[s3DirPrefix+localFile.Name()]; !ok {
			log.Printf("File %v is not in S3 bucket\n", localFile.Name())
			return
		}
		s3ObjMap[s3DirPrefix+localFile.Name()] = true
	}

	// 全て一致しているか確認
	for key, value := range s3ObjMap {
		if !value {
			log.Printf("File %v is not in S3 bucket\n", key)
			return
		}
	}

	fmt.Println("Pass: All files are in S3 bucket")
}

type BucketBasics struct {
	S3Client *s3.Client
}

func (basics BucketBasics) ListObjects(bucketName string) ([]types.Object, error) {
	result, err := basics.S3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	var contents []types.Object
	if err != nil {
		log.Printf("Couldn't list objects in bucket %v. Here's why: %v\n", bucketName, err)
	} else {
		contents = result.Contents
	}

	return contents, err
}
