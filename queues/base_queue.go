package queues

import (
	"encoding/xml"
)

type BaseQueue struct {
}

type xmlMessage struct {
	XMLName xml.Name `xml:"Message"`
	//Xmlns                  string   `xml:"xmlns,attr"`
	MessageId        string `xml:"MessageId"`
	ReceiptHandle    string `xml:"ReceiptHandle"`
	MessageBodyMD5   string `xml:"MessageBodyMD5"`
	MessageBody      string `xml:"MessageBody"`
	EnqueueTime      int    `xml:"EnqueueTime"`
	NextVisibleTime  int    `xml:"NextVisibleTime"`
	FirstDequeueTime int    `xml:"FirstDequeueTime"`
	DequeueCount     int    `xml:"DequeueCount"`
	Priority         int    `xml:"Priority"`
}

func init() {

}
