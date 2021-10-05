package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/crypto/ssh/terminal"
)

// globals
var globalCounter int64
var initTime int64

type qqData struct {
	ID           uint8  `json:"id"`
	Label        string `json:"label"`
	MacAddress   string `json:"mac_address"`
	ModelNumber  string `json:"model_number"`
	NodeName     string `json:"node_name"`
	NodeStatus   string `json:"node_status"`
	SerialNumber string `json:"serial_number"`
	UUID         string `json:"uuid"`
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func dataTransfer(input int64) (float64, float64) {
	nowTime := time.Now().Unix()
	elapsedTime := float64(initTime - nowTime)
	sumTransferred := float64(atomic.AddInt64(&globalCounter, input))
	avgSpeed := math.Abs((sumTransferred / elapsedTime) / 1024.0)

	return sumTransferred, avgSpeed
}

func divmod(numerator, denominator int64) (quotient, remainder int64) {
	quotient = numerator / denominator // integer division, decimals are truncated
	remainder = numerator % denominator
	return
}

func getNodes() []qqData {

	var out bytes.Buffer
	qqCmd := exec.Command("/opt/qumulo/cli/qq", "nodes_list")
	qqCmd.Stdout = &out
	err := qqCmd.Run()
	if err != nil {
		exitErrorf("Unable to access qq. Verify that you are authenticated., %v", err)
	}

	qqJSON := []byte(out.String())
	var jsonData []qqData

	err = json.Unmarshal(qqJSON, &jsonData)
	if err != nil {
		exitErrorf("Unable to unmarshal json, %v", err)
	}

	return jsonData
}

func assignJob(nodeList []string, name string) []string {

	var buckets []string
	var myBuckets []string
	hostIndex := 0

	for i := 0; i < 16; i++ {
		h := fmt.Sprintf("%x", i)
		buckets = append(buckets, h)
	}

	println("Buckets:", len(buckets))

	quotient, remainder := divmod(16, int64(len(nodeList)))
	println("div / mod:", quotient, "/", remainder)

	for k, v := range nodeList {
		if name == v {
			hostIndex = k
		}
	}

	for x := 0; x < int(quotient); x++ {
		myBuckets = append(myBuckets, buckets[x*len(nodeList)+hostIndex])
	}

	if hostIndex < int(remainder) {
		myBuckets = append(myBuckets, buckets[int(quotient)*len(nodeList)+hostIndex])
	}

	println("hostIndex:", hostIndex)
	println("myBuckets:", len(myBuckets))

	return myBuckets

}

func s3upload(logger *log.Logger, basedir string, myBuckets []string, s3bucket string, region string, width int) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	svc := s3.New(sess)
	uploader := s3manager.NewUploader(sess)

	files, err := ioutil.ReadDir(basedir)

	if err != nil {
		exitErrorf("Unable to access filesystem, %v", err)
	}

	for _, file := range files {
		filePath := basedir + "/" + file.Name()

		if strings.HasPrefix(file.Name(), ".") {
			continue
		}

		if file.IsDir() {
			s3upload(logger, filePath, myBuckets, s3bucket, region, width)
		} else {
			sha1 := sha1.Sum([]byte(filePath))
			sha1String := hex.EncodeToString(sha1[:])
			bucket := sha1String[:1]

			for _, x := range myBuckets {

				if x == bucket {
					fileMeta, err := os.Stat(filePath)

					if err != nil {
						logger.Println("Cannot stat:", filePath)
					} else {
						msg := "UNK"
						fileSize := fileMeta.Size()
						fileModified := fileMeta.ModTime().Unix()

						objectHead, err := svc.HeadObject(&s3.HeadObjectInput{
							Bucket: aws.String(s3bucket),
							Key:    aws.String(filePath[1:]),
						})

						var objectSize int64
						objectModified := int64(0)

						if err != nil {
							msg = "NEW"
						} else {
							objectSize = *objectHead.ContentLength
							objectModified = objectHead.LastModified.Unix()
							msg = "MOD"
						}

						if fileModified < objectModified && fileSize == objectSize {
							fmt.Printf("\033[2K\r[SKIP: %.*s]", width-10, filePath)
						} else {
							f, err := os.Open(filePath)
							if err != nil {
								logger.Printf("failed to open file %q, %v", filePath, err)
							} else {

								result, err := uploader.Upload(&s3manager.UploadInput{
									Bucket: aws.String(s3bucket),
									Key:    aws.String(filePath[1:]),
									Body:   f,
								})

								if err != nil {
									logger.Printf("S3 upload failed, %v", err)
								} else {
									_ = result
									shortTransfer := float64(0)
									unit := "XX"
									totalTransfer, avgSpeed := dataTransfer(fileSize)
									if totalTransfer < 1024*1024 {
										shortTransfer = totalTransfer / 1024
										unit = "KB"
									} else if totalTransfer < 1024*1024*1024 {
										shortTransfer = totalTransfer / 1024 / 1024
										unit = "MB"
									} else {
										shortTransfer = totalTransfer / 1024 / 1024 / 1024
										unit = "GB"
									}
									fmt.Printf("\033[2K\r[XFER: %.2f %s AVG: %.2f KB/s] %s: %.*s", shortTransfer, unit, avgSpeed, msg, width-50, filePath)
								}
							}
						}
					}
				}
			}
		}
	}
}

func main() {

	width, height, err := terminal.GetSize(0)

	if err != nil {
		println("No terminal found")
		width = 100
	} else {
		println("Found terminal:", width, height)
	}

	initTime = time.Now().Unix()

	var basedir = flag.String("basedir", "", "the locally-available directory to source for the upload/download process")
	var s3bucket = flag.String("s3bucket", "", "source or destination S3 bucket")
	var region = flag.String("region", "", "AWS region")
	var action = flag.String("action", "", "specify 'upload' or 'download'")
	flag.Parse()

	if *basedir == "" || *s3bucket == "" || *region == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	f, err := os.OpenFile("/history/q2s3.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		exitErrorf("Unable to init logfile, %v", err)
	}
	defer f.Close()
	logger := log.New(f, "q2s3: ", log.LstdFlags)
	logger.Println("Starting up...")

	var nodeList []string
	nodes := getNodes()
	for _, QqData := range nodes {
		nodeList = append(nodeList, QqData.NodeName)
	}
	println("Cluster nodes:", len(nodeList))

	name, err := os.Hostname()
	myBuckets := assignJob(nodeList, name)
	if len(myBuckets) < 1 {
		println("No buckets assigned to this node")
		os.Exit(0)
	}
	for x := range myBuckets {
		println("Bucket", x+1, myBuckets[x])
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(*region)},
	)
	svc := s3.New(sess)
	result, err := svc.ListBuckets(nil)

	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	bucketConfirmed := 0
	for _, b := range result.Buckets {
		if aws.StringValue(b.Name) == *s3bucket {
			bucketConfirmed = 1
			fmt.Printf("* %s created on %s\n", aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
		}
	}
	if bucketConfirmed == 0 {
		exitErrorf("Unable to locate S3 Bucket, %v", err)
	}

	if *action == "upload" {
		println("Uploading", *basedir, "to", *s3bucket, "on", name)
		time.Sleep(2 * time.Second)
		s3upload(logger, *basedir, myBuckets, *s3bucket, *region, width)
		logger.Println("Processed", globalCounter, "bytes")
	} else if *action == "download" {
		println("Downloading", *s3bucket, "to", *basedir, "on", name)
		println("Download function not enabled")
		os.Exit(1)
	} else {
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Printf("\n\nDone\n")
	println("Processed", globalCounter, "bytes")

}
