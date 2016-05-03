package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	concurrency = 10
	awsRegion   = "us-east-1"
)

var objectsCount = 0

func worker(id int, jobs <-chan *s3.Object, downloader *s3manager.Downloader, awsBucket string, destDir string) {
	for object := range jobs {
		os.MkdirAll(path.Dir(path.Join(destDir, *object.Key)), 0777)
		file, err := os.Create(path.Join(destDir, *object.Key))
		if err != nil {
			log.Fatalln("Failed to create file", err)
		}
		defer file.Close()

		numBytes, err := downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(awsBucket),
				Key:    object.Key,
			})

		file.Close()

		if err != nil {
			fmt.Println("Failed to download file", err)
			return
		}

		fmt.Println("worker", id, "downloaded", file.Name(), numBytes, "bytes", objectsCount)
	}
}

func main() {
	s3url := flag.String("url", "", "The s3 url to fetch from, e.g s3://foo/bar")
	destDir := flag.String("output", "", "The directory to output to")
	flag.Parse()

	jobs := make(chan *s3.Object)

	u, err := url.Parse(*s3url)
	if err != nil {
		log.Fatalln(err)
	}

	s3Bucket := u.Host
	s3Prefix := u.Path[1:]

	err = os.MkdirAll(*destDir, 0700)
	if err != nil {
		log.Fatalln(err)
	}

	awsConfig := &aws.Config{Region: aws.String(awsRegion)}
	awsSession := session.New(awsConfig)

	svc := s3.New(awsSession)
	downloader := s3manager.NewDownloader(awsSession)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(s3Prefix),
	}

	var wg sync.WaitGroup
	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			worker(w, jobs, downloader, s3Bucket, *destDir)
			defer wg.Done()
		}(w)
	}

	log.Printf("Looking for objects in bucket: %s, prefix: %s", s3Bucket, s3Prefix)

	err = svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		for _, object := range page.Contents {
			jobs <- object
		}
		objectsCount += len(page.Contents)
		return true
	})

	close(jobs)
	wg.Wait()

	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("Found %d objects to download.\n", objectsCount)
}
