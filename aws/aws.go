package aws

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	// TODO: rewrite these to the aws-sdk versions
	goamzaws "github.com/goamz/goamz/aws"
	goamzs3 "github.com/goamz/goamz/s3"
	goamzsqs "github.com/goamz/goamz/sqs"
	"github.com/prometheus/common/log"
)

type S3Type struct {
	ContentType string `json:"content_type"`
	Bucket      string `json:"bucket"`
	FileName    string `json:"file_name"`
	Id          string `json:"id"`
	PageNumber  string `json:"page_number"`
}

func (goamzs3 S3Type) FullPath() string {
	return fmt.Sprintf("%s/%s", goamzs3.Bucket, goamzs3.FileName)
}

func ListBucket(bucketName string) {
	auth, err := goamzaws.EnvAuth()
	if err != nil {
		log.Error(err)
	}

	connection := goamzs3.New(auth, goamzaws.USEast)
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
	auth, err := goamzaws.EnvAuth()
	if err != nil {
		log.Error(err)
		return err
	}

	connection := goamzs3.New(auth, goamzaws.USEast)
	bucket := connection.Bucket(obj.Bucket)

	if bucket.Put(obj.FileName, bytes, obj.ContentType, goamzs3.ACL("public-read"), goamzs3.Options{}); err != nil {
		return err
	}

	return nil
}

func Download(obj S3Type) error {
	log.Info("Downloading:", obj)
	auth, err := goamzaws.EnvAuth()
	if err != nil {
		log.Error(err)
		return err
	}

	connection := goamzs3.New(auth, goamzaws.USEast)
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

func GenerateSignedURL(obj S3Type) (string, error) {
	svc := s3.New(session.New(&aws.Config{Region: aws.String("us-east-1")}))
	req, _ := svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(obj.Bucket),
		Key:    aws.String(obj.FileName),
	})

	signedURL, err := req.Presign(15 * time.Minute)
	if err != nil {
		log.Error(err, " for", obj.FileName)
		return "", err
	}

	return signedURL, nil
	// auth, err := aws.EnvAuth()
	// if err != nil {
	// 	log.Error(err)
	// 	return "", err
	// }
	//
	// connection := goamzs3.New(auth, aws.USEast)
	// bucket := connection.Bucket(obj.Bucket)
	//
	// signedURL := bucket.UploadSignedURL(obj.FileName, "PUT", obj.ContentType, time.Now().Add(time.Minute*10))
	// if signedURL == "" {
	// 	err = errors.New("A signed URL failed to be generated")
	// 	log.Error(err, " for", obj.FileName)
	// 	return "", err
	// }
	//
}

func GetFromSQS(obj S3Type) (goamzsqs.ReceiveMessageResponse, error) {
	auth, err := goamzaws.EnvAuth()
	if err != nil {
		log.Error(err)
		return goamzsqs.ReceiveMessageResponse{}, err
	}

	// FIXME: hardcoded region: https://github.com/goamz/goamz/blob/master goamzsqs goamzsqs.go#L41
	conn, err := goamzsqs.NewFrom(auth.AccessKey, auth.SecretKey, "us.east")
	if err != nil {
		return goamzsqs.ReceiveMessageResponse{}, err
	}

	q, err := conn.GetQueue("pipedream-queue")
	if err != nil {
		return goamzsqs.ReceiveMessageResponse{}, err
	}
	log.Info("Receiving message from ", q.Name)

	resp, err := q.ReceiveMessage(1)
	if err != nil {
		return goamzsqs.ReceiveMessageResponse{}, err
	}

	return *resp, nil
}

// func SignURL(obj S3Type) string {
// 	b := connection.Bucket(obj.Bucket)
// 	return
// }

func InterfaceToS3Type(inter interface{}) S3Type {
	var next interface{}
	header := parseMap(inter.(map[string]interface{}))
	next = inter.(map[string]interface{})[header]

	s3 := S3Type{next.(map[string]interface{})["content_type"].(string), next.(map[string]interface{})["bucket"].(string), next.(map[string]interface{})["file_name"].(string), next.(map[string]interface{})["id"].(string), next.(map[string]interface{})["page_number"].(string)}

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
