package s3

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Error struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (e Error) Error() string {
	return fmt.Sprintf("%s (%s)", e.Message, e.Code)
}

func ResponseError(res *http.Response) error {
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return ResponseErrorFrom(b)
}

func ResponseErrorFrom(b []byte) error {
	resperr := Error{}
	if err := xml.Unmarshal(b, &resperr); err != nil {
		return fmt.Errorf("unable to parse response xml: %s", err)
	}

	return resperr
}
