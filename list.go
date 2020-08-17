package s3

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"time"
)

func (c *Client) List(prefix string) ([]Object, error) {
	objects := make([]Object, 0)
	params := ""
	if prefix != "" {
		params = fmt.Sprintf("&prefix=%s", prefix)
	}
	ctok := ""
	for {
		res, err := c.get(fmt.Sprintf("/?list-type=2&fetch-owner=true%s%s", params, ctok), nil)
		if err != nil {
			return nil, err
		}

		var r struct {
			XMLName  xml.Name `xml:"ListBucketResult"`
			Next     string   `xml:"NextContinuationToken"`
			Contents []struct {
				Key          string `xml:"Key"`
				LastModified string `xml:"LastModified"`
				ETag         string `xml:"ETag"`
				Size         int64  `xml:"Size"`
				StorageClass string `xml:"StorageClass"`
				Owner        struct {
					ID          string `xml:"ID"`
					DisplayName string `xml:"DisplayName"`
				} `xml:"Owner"`
			} `xml:"Contents"`
		}
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}

		if res.StatusCode != 200 {
			return nil, ResponseErrorFrom(b)
		}

		err = xml.Unmarshal(b, &r)
		if err != nil {
			return nil, err
		}

		for _, f := range r.Contents {
			mod, _ := time.Parse("2006-01-02T15:04:05.000Z", f.LastModified)
			objects = append(objects, Object{
				Key:          f.Key,
				LastModified: mod,
				ETag:         f.ETag[1 : len(f.ETag)-1],
				Size:         Bytes(f.Size),
				StorageClass: f.StorageClass,
				OwnerID:      f.Owner.ID,
				OwnerName:    f.Owner.DisplayName,
			})
		}

		if r.Next == "" {
			return objects, nil
		}

		ctok = fmt.Sprintf("&continuation-token=%s", r.Next)
	}
}

type ListIter struct {
	c       *Client
	objects []*Object
	ctok    string
	params  string
}

func (li *ListIter) refill() error {
	if li.ctok == " " {
		li.ctok = ""
	}
	res, err := li.c.get(fmt.Sprintf("/?list-type=2&fetch-owner=true%s%s", li.params, li.ctok), nil)
	if err != nil {
		return err
	}

	var r struct {
		XMLName  xml.Name `xml:"ListBucketResult"`
		Next     string   `xml:"NextContinuationToken"`
		Contents []struct {
			Key          string `xml:"Key"`
			LastModified string `xml:"LastModified"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
			StorageClass string `xml:"StorageClass"`
			Owner        struct {
				ID          string `xml:"ID"`
				DisplayName string `xml:"DisplayName"`
			} `xml:"Owner"`
		} `xml:"Contents"`
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return ResponseErrorFrom(b)
	}

	err = xml.Unmarshal(b, &r)
	if err != nil {
		return err
	}

	for _, f := range r.Contents {
		mod, _ := time.Parse("2006-01-02T15:04:05.000Z", f.LastModified)
		li.objects = append(li.objects, &Object{
			Key:          f.Key,
			LastModified: mod,
			ETag:         f.ETag[1 : len(f.ETag)-1],
			Size:         Bytes(f.Size),
			StorageClass: f.StorageClass,
			OwnerID:      f.Owner.ID,
			OwnerName:    f.Owner.DisplayName,
		})
	}

	if r.Next == "" {
		li.ctok = ""
		return nil
	}

	li.ctok = fmt.Sprintf("&continuation-token=%s", r.Next)
	return nil
}

func (li *ListIter) Next() (*Object, error) {
	if len(li.objects) == 0 {
		if li.ctok == "" {
			return nil, nil
		}
		err := li.refill()
		if err != nil {
			return nil, err
		}
	}
	o := li.objects[0]
	li.objects = li.objects[1:]
	return o, nil
}

func (c *Client) ListIter(prefix string) *ListIter {
	params := ""
	if prefix != "" {
		params = fmt.Sprintf("&prefix=%s", prefix)
	}
	li := &ListIter{
		c:      c,
		params: params,
		ctok:   " ",
	}
	return li
}
