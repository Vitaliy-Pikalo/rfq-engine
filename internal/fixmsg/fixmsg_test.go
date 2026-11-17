package fixmsg

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func sample() *Message {
	return New(MsgQuoteRequest).
		Set(TagQuoteReqID, "rfq-1").
		Set(TagSymbol, "BTC-USD").
		Set(TagSide, "1").
		SetFloat(TagOrderQty, 2.5)
}

func TestRoundTrip(t *testing.T) {
	b, err := sample().Encode()
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewReader(bytes.NewReader(b)).Read()
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != MsgQuoteRequest {
		t.Fatalf("type=%q", m.Type)
	}
	if q, _ := m.Float(TagOrderQty); q != 2.5 {
		t.Fatalf("qty=%v", q)
	}
	if s, _ := m.Get(TagSymbol); s != "BTC-USD" {
		t.Fatalf("symbol=%q", s)
	}
}

// Known-good frame computed by hand: checks BodyLength and CheckSum math
// against the FIX definition, not just against our own encoder.
func TestEncodeKnownFrame(t *testing.T) {
	got := New("0").String() // heartbeat, empty body
	want := "8=FIX.4.4|9=5|35=0|10=163|"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestStreamOfMessages(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		b, _ := sample().Encode()
		buf.Write(b)
	}
	r := NewReader(&buf)
	for i := 0; i < 3; i++ {
		if _, err := r.Read(); err != nil {
			t.Fatalf("msg %d: %v", i, err)
		}
	}
	if _, err := r.Read(); err != io.EOF {
		t.Fatalf("err=%v, want io.EOF at clean end", err)
	}
}

func TestDecodeErrors(t *testing.T) {
	good, _ := sample().Encode()
	cases := map[string][]byte{
		"bad checksum":     bytes.Replace(good, []byte("BTC"), []byte("ETH"), 1),
		"truncated":        good[:len(good)-4],
		"wrong begin":      []byte("8=FIX.4.2\x019=5\x0135=0\x0110=000\x01"),
		"huge body length": []byte("8=FIX.4.4\x019=999999999\x01"),
		"negative length":  []byte("8=FIX.4.4\x019=-1\x01"),
		"non-numeric tag":  []byte("8=FIX.4.4\x01x=5\x01"),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewReader(bytes.NewReader(in)).Read()
			if err == nil || err == io.EOF {
				t.Fatalf("err=%v, want a decode error", err)
			}
		})
	}
	_, err := NewReader(bytes.NewReader(cases["bad checksum"])).Read()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("err=%v, want ErrChecksum", err)
	}
}

func TestEncodeRejectsSOHInValue(t *testing.T) {
	if _, err := New("S").Set(TagText, "a\x01b").Encode(); err == nil {
		t.Fatal("want error for SOH inside a value")
	}
}

func TestMustReportsMissingTag(t *testing.T) {
	_, err := New("R").Must(TagSymbol)
	if err == nil || !strings.Contains(err.Error(), "55") {
		t.Fatalf("err=%v, want mention of tag 55", err)
	}
}

func FuzzReader(f *testing.F) {
	b, _ := sample().Encode()
	f.Add(b)
	f.Add([]byte("8=FIX.4.4\x019=5\x0135=0\x0110=163\x01"))
	f.Fuzz(func(t *testing.T, in []byte) {
		r := NewReader(bytes.NewReader(in))
		for i := 0; i < 4; i++ { // must never panic or hang
			if _, err := r.Read(); err != nil {
				return
			}
		}
	})
}
