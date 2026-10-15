package encoding

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
)

// Row format v2, copied from TiDB: values are addressed by column ID through a
// sorted ID array, so ADD and DROP COLUMN never rewrite rows (D3).
//
//	byte 0      format: RowFormatV2 (128)
//	byte 1      flags: bit 0 = large (u32 ids and offsets instead of u8 ids / u16 offsets)
//	u16         number of non-NULL columns
//	u16         number of NULL columns
//	ids         non-NULL column ids, ascending, then NULL column ids, ascending
//	offsets     end offset of each non-NULL value in data
//	data        the concatenated values
//
// A missing id means "the column's original default". NULL is listed explicitly
// because the default may not be NULL.
const (
	RowFormatV2  byte = 128
	DeleteMarker byte = 0x81

	rowFlagLarge byte = 1

	// MaxRowSize is the v1 limit on an encoded row.
	MaxRowSize = 1 << 20
)

var (
	ErrRowTooLarge = errors.New("encoding: row larger than 1 MiB")
	ErrBadRow      = errors.New("encoding: corrupt row")
)

// IsDelete reports whether a stored value is a delete marker version.
func IsDelete(v []byte) bool { return len(v) == 1 && v[0] == DeleteMarker }

// RowValue is one column of a row: Null, or the compact value bytes.
type RowValue struct {
	ID    uint32
	Null  bool
	Bytes []byte
}

// EncodeRow encodes the given columns. The slice is sorted in place by id.
func EncodeRow(cols []RowValue) ([]byte, error) {
	sort.Slice(cols, func(i, j int) bool { return cols[i].ID < cols[j].ID })
	var notNull, null []RowValue
	dataLen := 0
	large := false
	for _, c := range cols {
		if c.ID > math.MaxUint8 {
			large = true
		}
		if c.Null {
			null = append(null, c)
		} else {
			notNull = append(notNull, c)
			dataLen += len(c.Bytes)
		}
	}
	if dataLen > math.MaxUint16 {
		large = true
	}
	if len(notNull) > math.MaxUint16 || len(null) > math.MaxUint16 {
		return nil, ErrBadRow
	}
	idSize, offSize := 1, 2
	flags := byte(0)
	if large {
		idSize, offSize = 4, 4
		flags |= rowFlagLarge
	}
	size := 6 + (len(notNull)+len(null))*idSize + len(notNull)*offSize + dataLen
	if size > MaxRowSize {
		return nil, ErrRowTooLarge
	}
	b := make([]byte, 0, size)
	b = append(b, RowFormatV2, flags)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(notNull)))
	b = binary.LittleEndian.AppendUint16(b, uint16(len(null)))
	appendID := func(id uint32) {
		if large {
			b = binary.LittleEndian.AppendUint32(b, id)
		} else {
			b = append(b, byte(id))
		}
	}
	for _, c := range notNull {
		appendID(c.ID)
	}
	for _, c := range null {
		appendID(c.ID)
	}
	off := 0
	for _, c := range notNull {
		off += len(c.Bytes)
		if large {
			b = binary.LittleEndian.AppendUint32(b, uint32(off))
		} else {
			b = binary.LittleEndian.AppendUint16(b, uint16(off))
		}
	}
	for _, c := range notNull {
		b = append(b, c.Bytes...)
	}
	return b, nil
}

// Row is a decoded view over an encoded row; it does not copy the data.
type Row struct {
	large   bool
	notNull int
	null    int
	ids     []byte
	offsets []byte
	data    []byte
}

// DecodeRow parses the row header.
func DecodeRow(b []byte) (*Row, error) {
	if len(b) < 6 || b[0] != RowFormatV2 {
		return nil, ErrBadRow
	}
	r := &Row{large: b[1]&rowFlagLarge != 0}
	r.notNull = int(binary.LittleEndian.Uint16(b[2:]))
	r.null = int(binary.LittleEndian.Uint16(b[4:]))
	idSize, offSize := 1, 2
	if r.large {
		idSize, offSize = 4, 4
	}
	p := 6
	idEnd := p + (r.notNull+r.null)*idSize
	offEnd := idEnd + r.notNull*offSize
	if offEnd > len(b) {
		return nil, ErrBadRow
	}
	r.ids, r.offsets, r.data = b[p:idEnd], b[idEnd:offEnd], b[offEnd:]
	return r, nil
}

func (r *Row) id(i int) uint32 {
	if r.large {
		return binary.LittleEndian.Uint32(r.ids[i*4:])
	}
	return uint32(r.ids[i])
}

func (r *Row) end(i int) int {
	if r.large {
		return int(binary.LittleEndian.Uint32(r.offsets[i*4:]))
	}
	return int(binary.LittleEndian.Uint16(r.offsets[i*2:]))
}

// Column state returned by Lookup.
type ColState int

const (
	ColMissing ColState = iota // not in the row: use the column's original default
	ColNull
	ColValue
)

// Lookup binary-searches the id arrays for a column.
func (r *Row) Lookup(id uint32) (ColState, []byte, error) {
	i := sort.Search(r.notNull, func(i int) bool { return r.id(i) >= id })
	if i < r.notNull && r.id(i) == id {
		start := 0
		if i > 0 {
			start = r.end(i - 1)
		}
		end := r.end(i)
		if start > end || end > len(r.data) {
			return ColMissing, nil, ErrBadRow
		}
		return ColValue, r.data[start:end], nil
	}
	j := sort.Search(r.null, func(j int) bool { return r.id(r.notNull+j) >= id })
	if j < r.null && r.id(r.notNull+j) == id {
		return ColNull, nil, nil
	}
	return ColMissing, nil, nil
}

// ---- compact value encodings (not sortable) ---------------------------------

// ValueInt encodes an integer as 8 bytes little-endian.
func ValueInt(v int64) []byte { return binary.LittleEndian.AppendUint64(nil, uint64(v)) }

// ValueUint encodes an unsigned integer as 8 bytes little-endian.
func ValueUint(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }

// ValueFloat encodes an IEEE double.
func ValueFloat(v float64) []byte { return ValueUint(math.Float64bits(v)) }

// ValueDecimal encodes scale (u8) followed by the unscaled integer as
// big-endian two's-complement bytes.
func ValueDecimal(scale uint8, unscaled []byte) []byte {
	return append([]byte{scale}, unscaled...)
}

// ParseInt decodes ValueInt.
func ParseInt(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, ErrBadRow
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

// ParseUint decodes ValueUint.
func ParseUint(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, ErrBadRow
	}
	return binary.LittleEndian.Uint64(b), nil
}

// ParseFloat decodes ValueFloat.
func ParseFloat(b []byte) (float64, error) {
	u, err := ParseUint(b)
	return math.Float64frombits(u), err
}
