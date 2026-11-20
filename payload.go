package taskmill

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf8"
)

// This file holds the encoding of a task's payload and metadata as JSON,
// and the checks that jsonb can store them within their limits, before any
// statement runs.

// encodeMetadata encodes a task's metadata as JSON, which jsonb can store.
func encodeMetadata(meta map[string]string) ([]byte, error) {
	if len(meta) == 0 {
		return []byte("{}"), nil
	}
	text, err := encodeJSON(meta)
	if err != nil {
		return nil, invalidArgument("encode metadata: %v", err)
	}
	if len(text) > maxMetadataBytes {
		return nil, invalidArgument("metadata is %d bytes, more than the limit of %d", len(text), maxMetadataBytes)
	}
	if hasNUL(text) {
		return nil, invalidArgument("metadata contains a NUL character")
	}
	return text, nil
}

// encodeJSON encodes v as JSON without escaping &, <, and >, which jsonb
// stores as they are, so that the size limits count what is stored.
// encoding/json still escapes U+2028 and U+2029.
func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// encodePayload encodes args as JSON, and checks that jsonb can store it
// within the payload limit. The checks run before the query, so a payload
// that fails them leaves the caller's transaction usable.
func encodePayload(kind string, args Args) ([]byte, error) {
	payload, err := encodeJSON(args)
	if err != nil {
		return nil, invalidArgument("encode payload of kind %q: %v", kind, err)
	}
	size, numbersFit := storedSize(payload)
	switch {
	case bytes.Equal(payload, []byte("null")):
		return nil, invalidArgument("payload of kind %q encodes to null", kind)
	case size > maxPayloadBytes:
		return nil, invalidArgument("payload of kind %q is more than %d bytes as stored", kind, maxPayloadBytes)
	case !numbersFit:
		return nil, invalidArgument("payload of kind %q holds a number beyond the range of PostgreSQL's numeric type", kind)
	case !utf8.Valid(payload):
		return nil, invalidArgument("payload of kind %q is not valid UTF-8", kind)
	case hasNUL(payload):
		return nil, invalidArgument("payload of kind %q contains a NUL character", kind)
	}
	return payload, nil
}

// Limits of PostgreSQL's numeric type, in which jsonb stores numbers.
const (
	maxNumericWhole = 131_072 // digits before the point
	maxNumericScale = 16_383  // digits after the point
)

// storedSize returns at most the size of JSON text as PostgreSQL writes it
// out of jsonb, where each number is written in full: 1e6 becomes 1000000,
// so a short payload could grow past any limit, up to values that PostgreSQL
// cannot read back. Strings are written as they came. jsonb also adds a
// space after each comma and colon; that at most doubles the size, and the
// limit leaves it out, as it always has. Counting stops past the limit.
// numbersFit reports whether every number counted fits PostgreSQL's numeric
// type.
func storedSize(text []byte) (size int, numbersFit bool) {
	size, numbersFit = len(text), true
	inString := false
	for i := 0; i < len(text) && size <= maxPayloadBytes && numbersFit; i++ {
		switch c := text[i]; {
		case inString && c == '\\':
			i++ // the escaped byte cannot end the string
		case c == '"':
			inString = !inString
		case !inString && (c == '-' || '0' <= c && c <= '9'):
			end := i + 1
			for end < len(text) && strings.IndexByte("+-.0123456789Ee", text[end]) >= 0 {
				end++
			}
			var n int
			n, numbersFit = numberSize(text[i:end])
			size += n - (end - i)
			i = end - 1
		}
	}
	return size, numbersFit
}

// numberSize returns at most the length of a JSON number as PostgreSQL
// writes it: without an exponent, and with as many digits after the point as
// the number has. fits reports whether the number fits PostgreSQL's numeric
// type.
func numberSize(num []byte) (size int, fits bool) {
	digits := bytes.TrimPrefix(num, []byte("-"))
	sign := len(num) - len(digits)
	exp := 0
	if at := bytes.IndexAny(digits, "eE"); at >= 0 {
		var err error
		if exp, err = strconv.Atoi(string(digits[at+1:])); err != nil {
			return 0, false // beyond the range of int
		}
		digits = digits[:at]
	}
	// Past this bound, a number is beyond numeric's range, or a zero; within
	// it, the sums below cannot overflow.
	bound := len(digits) + maxNumericWhole + 1
	exp = max(-bound, min(exp, bound))
	whole, frac, _ := bytes.Cut(digits, []byte("."))
	zeros := len(whole) - len(bytes.TrimLeft(whole, "0"))
	if zeros == len(whole) {
		zeros += len(frac) - len(bytes.TrimLeft(frac, "0"))
	}
	before := 1 // digits before the point
	if zeros < len(whole)+len(frac) {
		before = max(1, len(whole)+exp-zeros)
	}
	after := max(0, len(frac)-exp)
	size = sign + before
	if after > 0 {
		size += 1 + after
	}
	return size, before <= maxNumericWhole && after <= maxNumericScale
}

// hasNUL reports whether JSON text encodes a NUL character, which jsonb
// cannot store. An escape is one only if an odd number of backslashes leads
// to it: in \\u0000 the backslash itself is escaped.
func hasNUL(text []byte) bool {
	for i := 0; ; {
		j := bytes.Index(text[i:], []byte("u0000"))
		if j < 0 {
			return false
		}
		j += i
		slashes := 0
		for k := j - 1; k >= 0 && text[k] == '\\'; k-- {
			slashes++
		}
		if slashes%2 == 1 {
			return true
		}
		i = j + 1
	}
}
