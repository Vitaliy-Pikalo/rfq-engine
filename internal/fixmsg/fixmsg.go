// Package fixmsg is a small FIX 4.4-style tag=value codec. It frames
// messages with BeginString(8), BodyLength(9) and CheckSum(10) the way real
// FIX does, so it can be read off a TCP stream. It is not a full FIX engine:
// no sessions, sequence numbers, or resend logic.
package fixmsg

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// SOH is the FIX field delimiter.
const SOH = '\x01'

// BeginString is the protocol version tag value.
const BeginString = "FIX.4.4"

// MaxBodyLen caps BodyLength so a bad or hostile peer can't make us
// allocate unbounded memory.
const MaxBodyLen = 64 << 10

// Tags used by this service. Standard FIX tags plus two user-defined ones
// (the 5000-9999 range is reserved for that).
const (
	TagBeginString = 8
	TagBodyLength  = 9
	TagCheckSum    = 10
	TagClOrdID     = 11
	TagLastPx      = 31
	TagLastQty     = 32
	TagMsgType     = 35
	TagOrderQty    = 38
	TagOrdStatus   = 39
	TagSide        = 54
	TagSymbol      = 55
	TagText        = 58
	TagValidUntil  = 62
	TagQuoteID     = 117
	TagQuoteReqID  = 131
	TagBidPx       = 132
	TagOfferPx     = 133
	TagPartyID     = 448 // dealer name
	TagQuoteCount  = 9001
	TagQuoteRank   = 9002
)

// Message types.
const (
	MsgQuoteRequest       = "R"
	MsgQuote              = "S"
	MsgQuoteRequestReject = "AG"
	MsgNewOrderSingle     = "D" // used as "accept this quote"
	MsgExecutionReport    = "8"
	MsgReject             = "3"
)

// OrdStatus values.
const (
	OrdStatusFilled   = "2"
	OrdStatusRejected = "8"
)

// Field is one tag=value pair.
type Field struct {
	Tag   int
	Value string
}

// Message is an ordered list of body fields plus its MsgType.
// Header and trailer (8, 9, 10) are added by Encode and stripped by Decode.
type Message struct {
	Type   string
	Fields []Field
}

// New starts a message of the given type.
func New(msgType string) *Message { return &Message{Type: msgType} }

// Set appends a field and returns m for chaining.
func (m *Message) Set(tag int, value string) *Message {
	m.Fields = append(m.Fields, Field{tag, value})
	return m
}

// SetFloat appends a float field using the shortest exact representation.
func (m *Message) SetFloat(tag int, v float64) *Message {
	return m.Set(tag, strconv.FormatFloat(v, 'f', -1, 64))
}

// Get returns the first value for tag.
func (m *Message) Get(tag int) (string, bool) {
	for _, f := range m.Fields {
		if f.Tag == tag {
			return f.Value, true
		}
	}
	return "", false
}

// Must returns the value for tag or an error naming the missing tag.
func (m *Message) Must(tag int) (string, error) {
	v, ok := m.Get(tag)
	if !ok || v == "" {
		return "", fmt.Errorf("fixmsg: %s missing required tag %d", m.Type, tag)
	}
	return v, nil
}

// Float parses a required float field.
func (m *Message) Float(tag int) (float64, error) {
	v, err := m.Must(tag)
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("fixmsg: tag %d: %w", tag, err)
	}
	return f, nil
}

// Int parses a required int field.
func (m *Message) Int(tag int) (int, error) {
	v, err := m.Must(tag)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("fixmsg: tag %d: %w", tag, err)
	}
	return n, nil
}

// Encode renders m as a framed FIX message.
func (m *Message) Encode() ([]byte, error) {
	var body bytes.Buffer
	writeField(&body, TagMsgType, m.Type)
	for _, f := range m.Fields {
		if strings.ContainsRune(f.Value, SOH) {
			return nil, fmt.Errorf("fixmsg: tag %d value contains SOH", f.Tag)
		}
		writeField(&body, f.Tag, f.Value)
	}
	var out bytes.Buffer
	writeField(&out, TagBeginString, BeginString)
	writeField(&out, TagBodyLength, strconv.Itoa(body.Len()))
	out.Write(body.Bytes())
	fmt.Fprintf(&out, "10=%03d%c", checksum(out.Bytes()), SOH)
	return out.Bytes(), nil
}

// String renders m with '|' for SOH, for logs and tests.
func (m *Message) String() string {
	b, err := m.Encode()
	if err != nil {
		return "<invalid: " + err.Error() + ">"
	}
	return string(bytes.ReplaceAll(b, []byte{SOH}, []byte{'|'}))
}

func writeField(b *bytes.Buffer, tag int, v string) {
	b.WriteString(strconv.Itoa(tag))
	b.WriteByte('=')
	b.WriteString(v)
	b.WriteByte(SOH)
}

func checksum(b []byte) int {
	sum := 0
	for _, c := range b {
		sum += int(c)
	}
	return sum % 256
}

// ErrChecksum is returned when the trailer checksum doesn't match.
var ErrChecksum = errors.New("fixmsg: bad checksum")

// Reader decodes framed messages from a stream.
type Reader struct {
	r *bufio.Reader
}

// NewReader wraps r.
func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReaderSize(r, 8<<10)} }

// Read returns the next message. io.EOF means the peer closed cleanly
// between messages; any other error means the stream is corrupt and the
// connection should be dropped.
func (rd *Reader) Read() (*Message, error) {
	var raw bytes.Buffer
	tag, val, err := rd.field(&raw)
	if err != nil {
		return nil, err // io.EOF passes through only here
	}
	if tag != TagBeginString || val != BeginString {
		return nil, fmt.Errorf("fixmsg: expected 8=%s, got %d=%q", BeginString, tag, val)
	}
	tag, val, err = rd.field(&raw)
	if err != nil {
		return nil, unexpectedEOF(err)
	}
	if tag != TagBodyLength {
		return nil, fmt.Errorf("fixmsg: expected tag 9, got %d", tag)
	}
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 || n > MaxBodyLen {
		return nil, fmt.Errorf("fixmsg: bad BodyLength %q", val)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(rd.r, body); err != nil {
		return nil, unexpectedEOF(err)
	}
	raw.Write(body)
	want := checksum(raw.Bytes())

	var trailer bytes.Buffer
	tag, val, err = rd.field(&trailer)
	if err != nil {
		return nil, unexpectedEOF(err)
	}
	if tag != TagCheckSum {
		return nil, fmt.Errorf("fixmsg: expected tag 10 after body, got %d (BodyLength wrong?)", tag)
	}
	got, err := strconv.Atoi(val)
	if err != nil || got != want {
		return nil, fmt.Errorf("%w: got %q want %03d", ErrChecksum, val, want)
	}
	return parseBody(body)
}

// field reads one "tag=value\x01" and appends the raw bytes to raw.
func (rd *Reader) field(raw *bytes.Buffer) (int, string, error) {
	line, err := rd.r.ReadSlice(SOH)
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return 0, "", errors.New("fixmsg: header field too long")
		}
		if err == io.EOF && len(line) > 0 {
			return 0, "", io.ErrUnexpectedEOF
		}
		return 0, "", err
	}
	raw.Write(line)
	return splitField(line[:len(line)-1])
}

func splitField(b []byte) (int, string, error) {
	i := bytes.IndexByte(b, '=')
	if i <= 0 {
		return 0, "", fmt.Errorf("fixmsg: malformed field %q", b)
	}
	tag, err := strconv.Atoi(string(b[:i]))
	if err != nil || tag <= 0 {
		return 0, "", fmt.Errorf("fixmsg: bad tag in %q", b)
	}
	return tag, string(b[i+1:]), nil
}

func parseBody(body []byte) (*Message, error) {
	if len(body) == 0 || body[len(body)-1] != SOH {
		return nil, errors.New("fixmsg: body must end with SOH")
	}
	parts := bytes.Split(body[:len(body)-1], []byte{SOH})
	m := &Message{}
	for i, p := range parts {
		tag, val, err := splitField(p)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			if tag != TagMsgType {
				return nil, fmt.Errorf("fixmsg: first body field must be 35, got %d", tag)
			}
			m.Type = val
			continue
		}
		m.Fields = append(m.Fields, Field{tag, val})
	}
	if m.Type == "" {
		return nil, errors.New("fixmsg: empty MsgType")
	}
	return m, nil
}

func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
