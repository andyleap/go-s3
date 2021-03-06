package s3

import (
	"net/http"
	"strconv"
	"time"
)

type Object struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         Bytes
	StorageClass string
	OwnerID      string
	OwnerName    string
}

func (c *Client) Head(key string) (*Object, error) {
	res, err := c.head(key, nil)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, ResponseError(res)
	}

	o := &Object{
		Key:  key,
		ETag: res.Header.Get("ETag"),
	}

	s, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	o.Size = Bytes(s)
	if err != nil {
		return nil, err
	}

	return o, nil
}

func (c *Client) Put(key string, body []byte, hdr *http.Header) error {
	res, err := c.put(key, body, hdr)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return ResponseError(res)
	}

	return nil
}
