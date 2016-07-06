package aws

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"github.com/goamz/goamz/sqs"
	"github.com/prometheus/common/log"
)

type S3Type struct {
	ContentType string `json:"content_type"`
	Bucket      string `json:"bucket"`
	FileName    string `json:"file_name"`
	Id          string `json:"id"`
}

func (s3 S3Type) FullPath() string {
	return fmt.Sprintf("%s/%s", s3.Bucket, s3.FileName)
}

func ListBucket(bucketName string) {
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Error(err)
	}

	connection := s3.New(auth, aws.USEast)
	bucket := connection.Bucket(bucketName)

	res, err := bucket.List("", "", "", 1000)
	if err != nil {
		log.Error(err)
	}

	for _, v := range res.Contents {
		fmt.Println(v.Key)
	}
}

func Upload(obj S3Type, bytes []byte) error {
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Error(err)
		return err
	}

	connection := s3.New(auth, aws.USEast)
	bucket := connection.Bucket(obj.Bucket)

	if bucket.Put(obj.FileName, bytes, obj.ContentType, s3.ACL("public-read"), s3.Options{}); err != nil {
		return err
	}

	return nil
}

func Download(obj S3Type) error {
	log.Info("Downloading:", obj)
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Error(err)
		return err
	}

	connection := s3.New(auth, aws.USEast)
	bucket := connection.Bucket(obj.Bucket)

	bytes, err := bucket.Get(obj.FileName)
	if err != nil {
		log.Error(err)
		return err
	}

	dlFile, err := os.Create(fmt.Sprintf("%s", obj.FileName))
	if err != nil {
		log.Error(err)
		return err
	}

	buffer := bufio.NewWriter(dlFile)
	buffer.Write(bytes)

	io.Copy(buffer, dlFile)

	return nil
}

func GetFromSQS(obj S3Type) (sqs.ReceiveMessageResponse, error) {
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Error(err)
		return sqs.ReceiveMessageResponse{}, err
	}

	// FIXME: hardcoded region: https://github.com/goamz/goamz/blob/master/sqs/sqs.go#L41
	conn, err := sqs.NewFrom(auth.AccessKey, auth.SecretKey, "us.east")
	if err != nil {
		return sqs.ReceiveMessageResponse{}, err
	}

	q, err := conn.GetQueue("pipedream-queue")
	if err != nil {
		return sqs.ReceiveMessageResponse{}, err
	}
	log.Info("Receiving message from ", q.Name)

	resp, err := q.ReceiveMessage(1)
	if err != nil {
		return sqs.ReceiveMessageResponse{}, err
	}

	return *resp, nil
}

func InterfaceToS3Type(inter interface{}) S3Type {
	var next interface{}
	header := parseMap(inter.(map[string]interface{}))
	next = inter.(map[string]interface{})[header]

	s3 := S3Type{next.(map[string]interface{})["content_type"].(string), next.(map[string]interface{})["bucket"].(string), next.(map[string]interface{})["file_name"].(string), next.(map[string]interface{})["id"].(string)}

	return s3
}

func parseMap(aMap map[string]interface{}) string {
	for key, val := range aMap {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			fmt.Println(key)
			return key
		case []interface{}:
			fmt.Println(key)
			parseArray(val.([]interface{}))
		default:
			fmt.Println(key, ":", concreteVal)
		}
	}
	return ""
}

func parseArray(anArray []interface{}) {
	for i, val := range anArray {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			fmt.Println("Index:", i)
			parseMap(val.(map[string]interface{}))
		case []interface{}:
			fmt.Println("Index:", i)
			parseArray(val.([]interface{}))
		default:
			fmt.Println("Index", i, ":", concreteVal)
		}
	}
}
