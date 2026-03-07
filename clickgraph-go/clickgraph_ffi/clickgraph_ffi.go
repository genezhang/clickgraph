package clickgraph_ffi

// #include <clickgraph_ffi.h>
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// This is needed, because as of go 1.24
// type RustBuffer C.RustBuffer cannot have methods,
// RustBuffer is treated as non-local type
type GoRustBuffer struct {
	inner C.RustBuffer
}

type RustBufferI interface {
	AsReader() *bytes.Reader
	Free()
	ToGoBytes() []byte
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

// C.RustBuffer fields exposed as an interface so they can be accessed in different Go packages.
// See https://github.com/golang/go/issues/13467
type ExternalCRustBuffer interface {
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

func RustBufferFromC(b C.RustBuffer) ExternalCRustBuffer {
	return GoRustBuffer{
		inner: b,
	}
}

func CFromRustBuffer(b ExternalCRustBuffer) C.RustBuffer {
	return C.RustBuffer{
		capacity: C.uint64_t(b.Capacity()),
		len:      C.uint64_t(b.Len()),
		data:     (*C.uchar)(b.Data()),
	}
}

func RustBufferFromExternal(b ExternalCRustBuffer) GoRustBuffer {
	return GoRustBuffer{
		inner: C.RustBuffer{
			capacity: C.uint64_t(b.Capacity()),
			len:      C.uint64_t(b.Len()),
			data:     (*C.uchar)(b.Data()),
		},
	}
}

func (cb GoRustBuffer) Capacity() uint64 {
	return uint64(cb.inner.capacity)
}

func (cb GoRustBuffer) Len() uint64 {
	return uint64(cb.inner.len)
}

func (cb GoRustBuffer) Data() unsafe.Pointer {
	return unsafe.Pointer(cb.inner.data)
}

func (cb GoRustBuffer) AsReader() *bytes.Reader {
	b := unsafe.Slice((*byte)(cb.inner.data), C.uint64_t(cb.inner.len))
	return bytes.NewReader(b)
}

func (cb GoRustBuffer) Free() {
	rustCall(func(status *C.RustCallStatus) bool {
		C.ffi_clickgraph_ffi_rustbuffer_free(cb.inner, status)
		return false
	})
}

func (cb GoRustBuffer) ToGoBytes() []byte {
	return C.GoBytes(unsafe.Pointer(cb.inner.data), C.int(cb.inner.len))
}

func stringToRustBuffer(str string) C.RustBuffer {
	return bytesToRustBuffer([]byte(str))
}

func bytesToRustBuffer(b []byte) C.RustBuffer {
	if len(b) == 0 {
		return C.RustBuffer{}
	}
	// We can pass the pointer along here, as it is pinned
	// for the duration of this call
	foreign := C.ForeignBytes{
		len:  C.int(len(b)),
		data: (*C.uchar)(unsafe.Pointer(&b[0])),
	}

	return rustCall(func(status *C.RustCallStatus) C.RustBuffer {
		return C.ffi_clickgraph_ffi_rustbuffer_from_bytes(foreign, status)
	})
}

type BufLifter[GoType any] interface {
	Lift(value RustBufferI) GoType
}

type BufLowerer[GoType any] interface {
	Lower(value GoType) C.RustBuffer
}

type BufReader[GoType any] interface {
	Read(reader io.Reader) GoType
}

type BufWriter[GoType any] interface {
	Write(writer io.Writer, value GoType)
}

func LowerIntoRustBuffer[GoType any](bufWriter BufWriter[GoType], value GoType) C.RustBuffer {
	// This might be not the most efficient way but it does not require knowing allocation size
	// beforehand
	var buffer bytes.Buffer
	bufWriter.Write(&buffer, value)

	bytes, err := io.ReadAll(&buffer)
	if err != nil {
		panic(fmt.Errorf("reading written data: %w", err))
	}
	return bytesToRustBuffer(bytes)
}

func LiftFromRustBuffer[GoType any](bufReader BufReader[GoType], rbuf RustBufferI) GoType {
	defer rbuf.Free()
	reader := rbuf.AsReader()
	item := bufReader.Read(reader)
	if reader.Len() > 0 {
		// TODO: Remove this
		leftover, _ := io.ReadAll(reader)
		panic(fmt.Errorf("Junk remaining in buffer after lifting: %s", string(leftover)))
	}
	return item
}

func rustCallWithError[E any, U any](converter BufReader[*E], callback func(*C.RustCallStatus) U) (U, *E) {
	var status C.RustCallStatus
	returnValue := callback(&status)
	err := checkCallStatus(converter, status)
	return returnValue, err
}

func checkCallStatus[E any](converter BufReader[*E], status C.RustCallStatus) *E {
	switch status.code {
	case 0:
		return nil
	case 1:
		return LiftFromRustBuffer(converter, GoRustBuffer{inner: status.errorBuf})
	case 2:
		// when the rust code sees a panic, it tries to construct a rustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{inner: status.errorBuf})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		panic(fmt.Errorf("unknown status code: %d", status.code))
	}
}

func checkCallStatusUnknown(status C.RustCallStatus) error {
	switch status.code {
	case 0:
		return nil
	case 1:
		panic(fmt.Errorf("function not returning an error returned an error"))
	case 2:
		// when the rust code sees a panic, it tries to construct a C.RustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: status.errorBuf,
			})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		return fmt.Errorf("unknown status code: %d", status.code)
	}
}

func rustCall[U any](callback func(*C.RustCallStatus) U) U {
	returnValue, err := rustCallWithError[error](nil, callback)
	if err != nil {
		panic(err)
	}
	return returnValue
}

type NativeError interface {
	AsError() error
}

func writeInt8(writer io.Writer, value int8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint8(writer io.Writer, value uint8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt16(writer io.Writer, value int16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint16(writer io.Writer, value uint16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt32(writer io.Writer, value int32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint32(writer io.Writer, value uint32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt64(writer io.Writer, value int64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint64(writer io.Writer, value uint64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat32(writer io.Writer, value float32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat64(writer io.Writer, value float64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func readInt8(reader io.Reader) int8 {
	var result int8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint8(reader io.Reader) uint8 {
	var result uint8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt16(reader io.Reader) int16 {
	var result int16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint16(reader io.Reader) uint16 {
	var result uint16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt32(reader io.Reader) int32 {
	var result int32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint32(reader io.Reader) uint32 {
	var result uint32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt64(reader io.Reader) int64 {
	var result int64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint64(reader io.Reader) uint64 {
	var result uint64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat32(reader io.Reader) float32 {
	var result float32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat64(reader io.Reader) float64 {
	var result float64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func init() {

	uniffiCheckChecksums()
}

func uniffiCheckChecksums() {
	// Get the bindings contract version from our ComponentInterface
	bindingsContractVersion := 29
	// Get the scaffolding contract version by calling the into the dylib
	scaffoldingContractVersion := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint32_t {
		return C.ffi_clickgraph_ffi_uniffi_contract_version()
	})
	if bindingsContractVersion != int(scaffoldingContractVersion) {
		// If this happens try cleaning and rebuilding your project
		panic("clickgraph_ffi: UniFFI contract version mismatch")
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_connection_export()
		})
		if checksum != 2862 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_connection_export: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_connection_export_to_sql()
		})
		if checksum != 32040 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_connection_export_to_sql: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_connection_query()
		})
		if checksum != 57594 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_connection_query: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_connection_query_to_sql()
		})
		if checksum != 49541 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_connection_query_to_sql: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_database_connect()
		})
		if checksum != 3694 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_database_connect: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_queryresult_column_names()
		})
		if checksum != 20724 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_queryresult_column_names: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_queryresult_get_all_rows()
		})
		if checksum != 39210 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_queryresult_get_all_rows: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_queryresult_get_next()
		})
		if checksum != 45662 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_queryresult_get_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_queryresult_has_next()
		})
		if checksum != 35692 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_queryresult_has_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_queryresult_num_rows()
		})
		if checksum != 51536 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_queryresult_num_rows: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_method_queryresult_reset()
		})
		if checksum != 31967 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_method_queryresult_reset: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_constructor_database_open()
		})
		if checksum != 41458 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_constructor_database_open: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_clickgraph_ffi_checksum_constructor_database_open_with_config()
		})
		if checksum != 8053 {
			// If this happens try cleaning and rebuilding your project
			panic("clickgraph_ffi: uniffi_clickgraph_ffi_checksum_constructor_database_open_with_config: UniFFI API checksum mismatch")
		}
	}
}

type FfiConverterUint32 struct{}

var FfiConverterUint32INSTANCE = FfiConverterUint32{}

func (FfiConverterUint32) Lower(value uint32) C.uint32_t {
	return C.uint32_t(value)
}

func (FfiConverterUint32) Write(writer io.Writer, value uint32) {
	writeUint32(writer, value)
}

func (FfiConverterUint32) Lift(value C.uint32_t) uint32 {
	return uint32(value)
}

func (FfiConverterUint32) Read(reader io.Reader) uint32 {
	return readUint32(reader)
}

type FfiDestroyerUint32 struct{}

func (FfiDestroyerUint32) Destroy(_ uint32) {}

type FfiConverterUint64 struct{}

var FfiConverterUint64INSTANCE = FfiConverterUint64{}

func (FfiConverterUint64) Lower(value uint64) C.uint64_t {
	return C.uint64_t(value)
}

func (FfiConverterUint64) Write(writer io.Writer, value uint64) {
	writeUint64(writer, value)
}

func (FfiConverterUint64) Lift(value C.uint64_t) uint64 {
	return uint64(value)
}

func (FfiConverterUint64) Read(reader io.Reader) uint64 {
	return readUint64(reader)
}

type FfiDestroyerUint64 struct{}

func (FfiDestroyerUint64) Destroy(_ uint64) {}

type FfiConverterInt64 struct{}

var FfiConverterInt64INSTANCE = FfiConverterInt64{}

func (FfiConverterInt64) Lower(value int64) C.int64_t {
	return C.int64_t(value)
}

func (FfiConverterInt64) Write(writer io.Writer, value int64) {
	writeInt64(writer, value)
}

func (FfiConverterInt64) Lift(value C.int64_t) int64 {
	return int64(value)
}

func (FfiConverterInt64) Read(reader io.Reader) int64 {
	return readInt64(reader)
}

type FfiDestroyerInt64 struct{}

func (FfiDestroyerInt64) Destroy(_ int64) {}

type FfiConverterFloat64 struct{}

var FfiConverterFloat64INSTANCE = FfiConverterFloat64{}

func (FfiConverterFloat64) Lower(value float64) C.double {
	return C.double(value)
}

func (FfiConverterFloat64) Write(writer io.Writer, value float64) {
	writeFloat64(writer, value)
}

func (FfiConverterFloat64) Lift(value C.double) float64 {
	return float64(value)
}

func (FfiConverterFloat64) Read(reader io.Reader) float64 {
	return readFloat64(reader)
}

type FfiDestroyerFloat64 struct{}

func (FfiDestroyerFloat64) Destroy(_ float64) {}

type FfiConverterBool struct{}

var FfiConverterBoolINSTANCE = FfiConverterBool{}

func (FfiConverterBool) Lower(value bool) C.int8_t {
	if value {
		return C.int8_t(1)
	}
	return C.int8_t(0)
}

func (FfiConverterBool) Write(writer io.Writer, value bool) {
	if value {
		writeInt8(writer, 1)
	} else {
		writeInt8(writer, 0)
	}
}

func (FfiConverterBool) Lift(value C.int8_t) bool {
	return value != 0
}

func (FfiConverterBool) Read(reader io.Reader) bool {
	return readInt8(reader) != 0
}

type FfiDestroyerBool struct{}

func (FfiDestroyerBool) Destroy(_ bool) {}

type FfiConverterString struct{}

var FfiConverterStringINSTANCE = FfiConverterString{}

func (FfiConverterString) Lift(rb RustBufferI) string {
	defer rb.Free()
	reader := rb.AsReader()
	b, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Errorf("reading reader: %w", err))
	}
	return string(b)
}

func (FfiConverterString) Read(reader io.Reader) string {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading string, expected %d, read %d", length, read_length))
	}
	return string(buffer)
}

func (FfiConverterString) Lower(value string) C.RustBuffer {
	return stringToRustBuffer(value)
}

func (c FfiConverterString) LowerExternal(value string) ExternalCRustBuffer {
	return RustBufferFromC(stringToRustBuffer(value))
}

func (FfiConverterString) Write(writer io.Writer, value string) {
	if len(value) > math.MaxInt32 {
		panic("String is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := io.WriteString(writer, value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing string, expected %d, written %d", len(value), write_length))
	}
}

type FfiDestroyerString struct{}

func (FfiDestroyerString) Destroy(_ string) {}

// Below is an implementation of synchronization requirements outlined in the link.
// https://github.com/mozilla/uniffi-rs/blob/0dc031132d9493ca812c3af6e7dd60ad2ea95bf0/uniffi_bindgen/src/bindings/kotlin/templates/ObjectRuntime.kt#L31

type FfiObject struct {
	pointer       unsafe.Pointer
	callCounter   atomic.Int64
	cloneFunction func(unsafe.Pointer, *C.RustCallStatus) unsafe.Pointer
	freeFunction  func(unsafe.Pointer, *C.RustCallStatus)
	destroyed     atomic.Bool
}

func newFfiObject(
	pointer unsafe.Pointer,
	cloneFunction func(unsafe.Pointer, *C.RustCallStatus) unsafe.Pointer,
	freeFunction func(unsafe.Pointer, *C.RustCallStatus),
) FfiObject {
	return FfiObject{
		pointer:       pointer,
		cloneFunction: cloneFunction,
		freeFunction:  freeFunction,
	}
}

func (ffiObject *FfiObject) incrementPointer(debugName string) unsafe.Pointer {
	for {
		counter := ffiObject.callCounter.Load()
		if counter <= -1 {
			panic(fmt.Errorf("%v object has already been destroyed", debugName))
		}
		if counter == math.MaxInt64 {
			panic(fmt.Errorf("%v object call counter would overflow", debugName))
		}
		if ffiObject.callCounter.CompareAndSwap(counter, counter+1) {
			break
		}
	}

	return rustCall(func(status *C.RustCallStatus) unsafe.Pointer {
		return ffiObject.cloneFunction(ffiObject.pointer, status)
	})
}

func (ffiObject *FfiObject) decrementPointer() {
	if ffiObject.callCounter.Add(-1) == -1 {
		ffiObject.freeRustArcPtr()
	}
}

func (ffiObject *FfiObject) destroy() {
	if ffiObject.destroyed.CompareAndSwap(false, true) {
		if ffiObject.callCounter.Add(-1) == -1 {
			ffiObject.freeRustArcPtr()
		}
	}
}

func (ffiObject *FfiObject) freeRustArcPtr() {
	rustCall(func(status *C.RustCallStatus) int32 {
		ffiObject.freeFunction(ffiObject.pointer, status)
		return 0
	})
}

type ConnectionInterface interface {
	// Export Cypher query results directly to a file.
	//
	// Supported formats: parquet, csv, tsv, json, ndjson.
	// Format is auto-detected from the file extension if not specified.
	Export(cypher string, outputPath string, options ExportOptions) error
	// Generate the export SQL without executing it (for debugging).
	ExportToSql(cypher string, outputPath string, options ExportOptions) (string, error)
	// Execute a Cypher query and return a QueryResult.
	Query(cypher string) (*QueryResult, error)
	// Translate a Cypher query to ClickHouse SQL without executing it.
	QueryToSql(cypher string) (string, error)
}
type Connection struct {
	ffiObject FfiObject
}

// Export Cypher query results directly to a file.
//
// Supported formats: parquet, csv, tsv, json, ndjson.
// Format is auto-detected from the file extension if not specified.
func (_self *Connection) Export(cypher string, outputPath string, options ExportOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*Connection")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[ClickGraphError](FfiConverterClickGraphError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_clickgraph_ffi_fn_method_connection_export(
			_pointer, FfiConverterStringINSTANCE.Lower(cypher), FfiConverterStringINSTANCE.Lower(outputPath), FfiConverterExportOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Generate the export SQL without executing it (for debugging).
func (_self *Connection) ExportToSql(cypher string, outputPath string, options ExportOptions) (string, error) {
	_pointer := _self.ffiObject.incrementPointer("*Connection")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[ClickGraphError](FfiConverterClickGraphError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_clickgraph_ffi_fn_method_connection_export_to_sql(
				_pointer, FfiConverterStringINSTANCE.Lower(cypher), FfiConverterStringINSTANCE.Lower(outputPath), FfiConverterExportOptionsINSTANCE.Lower(options), _uniffiStatus),
		}
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue string
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterStringINSTANCE.Lift(_uniffiRV), nil
	}
}

// Execute a Cypher query and return a QueryResult.
func (_self *Connection) Query(cypher string) (*QueryResult, error) {
	_pointer := _self.ffiObject.incrementPointer("*Connection")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[ClickGraphError](FfiConverterClickGraphError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_clickgraph_ffi_fn_method_connection_query(
			_pointer, FfiConverterStringINSTANCE.Lower(cypher), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *QueryResult
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterQueryResultINSTANCE.Lift(_uniffiRV), nil
	}
}

// Translate a Cypher query to ClickHouse SQL without executing it.
func (_self *Connection) QueryToSql(cypher string) (string, error) {
	_pointer := _self.ffiObject.incrementPointer("*Connection")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[ClickGraphError](FfiConverterClickGraphError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_clickgraph_ffi_fn_method_connection_query_to_sql(
				_pointer, FfiConverterStringINSTANCE.Lower(cypher), _uniffiStatus),
		}
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue string
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterStringINSTANCE.Lift(_uniffiRV), nil
	}
}
func (object *Connection) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterConnection struct{}

var FfiConverterConnectionINSTANCE = FfiConverterConnection{}

func (c FfiConverterConnection) Lift(pointer unsafe.Pointer) *Connection {
	result := &Connection{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_clickgraph_ffi_fn_clone_connection(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_clickgraph_ffi_fn_free_connection(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Connection).Destroy)
	return result
}

func (c FfiConverterConnection) Read(reader io.Reader) *Connection {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterConnection) Lower(value *Connection) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*Connection")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterConnection) Write(writer io.Writer, value *Connection) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerConnection struct{}

func (_ FfiDestroyerConnection) Destroy(value *Connection) {
	value.Destroy()
}

type DatabaseInterface interface {
	// Create a connection to this database.
	Connect() (*Connection, error)
}
type Database struct {
	ffiObject FfiObject
}

// Open a database from a YAML schema file with default configuration.
func DatabaseOpen(schemaPath string) (*Database, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[ClickGraphError](FfiConverterClickGraphError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_clickgraph_ffi_fn_constructor_database_open(FfiConverterStringINSTANCE.Lower(schemaPath), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Database
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterDatabaseINSTANCE.Lift(_uniffiRV), nil
	}
}

// Open a database from a YAML schema file with custom configuration.
func DatabaseOpenWithConfig(schemaPath string, config SystemConfig) (*Database, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[ClickGraphError](FfiConverterClickGraphError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_clickgraph_ffi_fn_constructor_database_open_with_config(FfiConverterStringINSTANCE.Lower(schemaPath), FfiConverterSystemConfigINSTANCE.Lower(config), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Database
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterDatabaseINSTANCE.Lift(_uniffiRV), nil
	}
}

// Create a connection to this database.
func (_self *Database) Connect() (*Connection, error) {
	_pointer := _self.ffiObject.incrementPointer("*Database")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[ClickGraphError](FfiConverterClickGraphError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_clickgraph_ffi_fn_method_database_connect(
			_pointer, _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Connection
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterConnectionINSTANCE.Lift(_uniffiRV), nil
	}
}
func (object *Database) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDatabase struct{}

var FfiConverterDatabaseINSTANCE = FfiConverterDatabase{}

func (c FfiConverterDatabase) Lift(pointer unsafe.Pointer) *Database {
	result := &Database{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_clickgraph_ffi_fn_clone_database(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_clickgraph_ffi_fn_free_database(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Database).Destroy)
	return result
}

func (c FfiConverterDatabase) Read(reader io.Reader) *Database {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterDatabase) Lower(value *Database) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*Database")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterDatabase) Write(writer io.Writer, value *Database) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerDatabase struct{}

func (_ FfiDestroyerDatabase) Destroy(value *Database) {
	value.Destroy()
}

type QueryResultInterface interface {
	// Column names in result order.
	ColumnNames() []string
	// Return all rows at once as a list of Row records.
	GetAllRows() []Row
	// Return the next row (cursor-style). Returns None when exhausted.
	GetNext() *Row
	// Return true if the cursor has more rows.
	HasNext() bool
	// Total number of rows.
	NumRows() uint64
	// Reset the cursor to the beginning.
	Reset()
}
type QueryResult struct {
	ffiObject FfiObject
}

// Column names in result order.
func (_self *QueryResult) ColumnNames() []string {
	_pointer := _self.ffiObject.incrementPointer("*QueryResult")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterSequenceStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_clickgraph_ffi_fn_method_queryresult_column_names(
				_pointer, _uniffiStatus),
		}
	}))
}

// Return all rows at once as a list of Row records.
func (_self *QueryResult) GetAllRows() []Row {
	_pointer := _self.ffiObject.incrementPointer("*QueryResult")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterSequenceRowINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_clickgraph_ffi_fn_method_queryresult_get_all_rows(
				_pointer, _uniffiStatus),
		}
	}))
}

// Return the next row (cursor-style). Returns None when exhausted.
func (_self *QueryResult) GetNext() *Row {
	_pointer := _self.ffiObject.incrementPointer("*QueryResult")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterOptionalRowINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_clickgraph_ffi_fn_method_queryresult_get_next(
				_pointer, _uniffiStatus),
		}
	}))
}

// Return true if the cursor has more rows.
func (_self *QueryResult) HasNext() bool {
	_pointer := _self.ffiObject.incrementPointer("*QueryResult")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterBoolINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.int8_t {
		return C.uniffi_clickgraph_ffi_fn_method_queryresult_has_next(
			_pointer, _uniffiStatus)
	}))
}

// Total number of rows.
func (_self *QueryResult) NumRows() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*QueryResult")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_clickgraph_ffi_fn_method_queryresult_num_rows(
			_pointer, _uniffiStatus)
	}))
}

// Reset the cursor to the beginning.
func (_self *QueryResult) Reset() {
	_pointer := _self.ffiObject.incrementPointer("*QueryResult")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_clickgraph_ffi_fn_method_queryresult_reset(
			_pointer, _uniffiStatus)
		return false
	})
}
func (object *QueryResult) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterQueryResult struct{}

var FfiConverterQueryResultINSTANCE = FfiConverterQueryResult{}

func (c FfiConverterQueryResult) Lift(pointer unsafe.Pointer) *QueryResult {
	result := &QueryResult{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_clickgraph_ffi_fn_clone_queryresult(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_clickgraph_ffi_fn_free_queryresult(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*QueryResult).Destroy)
	return result
}

func (c FfiConverterQueryResult) Read(reader io.Reader) *QueryResult {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterQueryResult) Lower(value *QueryResult) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*QueryResult")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterQueryResult) Write(writer io.Writer, value *QueryResult) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerQueryResult struct{}

func (_ FfiDestroyerQueryResult) Destroy(value *QueryResult) {
	value.Destroy()
}

type ExportOptions struct {
	// Format name: "parquet", "csv", "tsv", "json", "ndjson".
	// If None, auto-detected from the file extension.
	Format *string
	// Parquet compression codec: "snappy", "gzip", "lz4", "zstd".
	Compression *string
}

func (r *ExportOptions) Destroy() {
	FfiDestroyerOptionalString{}.Destroy(r.Format)
	FfiDestroyerOptionalString{}.Destroy(r.Compression)
}

type FfiConverterExportOptions struct{}

var FfiConverterExportOptionsINSTANCE = FfiConverterExportOptions{}

func (c FfiConverterExportOptions) Lift(rb RustBufferI) ExportOptions {
	return LiftFromRustBuffer[ExportOptions](c, rb)
}

func (c FfiConverterExportOptions) Read(reader io.Reader) ExportOptions {
	return ExportOptions{
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterExportOptions) Lower(value ExportOptions) C.RustBuffer {
	return LowerIntoRustBuffer[ExportOptions](c, value)
}

func (c FfiConverterExportOptions) LowerExternal(value ExportOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ExportOptions](c, value))
}

func (c FfiConverterExportOptions) Write(writer io.Writer, value ExportOptions) {
	FfiConverterOptionalStringINSTANCE.Write(writer, value.Format)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.Compression)
}

type FfiDestroyerExportOptions struct{}

func (_ FfiDestroyerExportOptions) Destroy(value ExportOptions) {
	value.Destroy()
}

type MapEntry struct {
	Key   string
	Value Value
}

func (r *MapEntry) Destroy() {
	FfiDestroyerString{}.Destroy(r.Key)
	FfiDestroyerValue{}.Destroy(r.Value)
}

type FfiConverterMapEntry struct{}

var FfiConverterMapEntryINSTANCE = FfiConverterMapEntry{}

func (c FfiConverterMapEntry) Lift(rb RustBufferI) MapEntry {
	return LiftFromRustBuffer[MapEntry](c, rb)
}

func (c FfiConverterMapEntry) Read(reader io.Reader) MapEntry {
	return MapEntry{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterValueINSTANCE.Read(reader),
	}
}

func (c FfiConverterMapEntry) Lower(value MapEntry) C.RustBuffer {
	return LowerIntoRustBuffer[MapEntry](c, value)
}

func (c FfiConverterMapEntry) LowerExternal(value MapEntry) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[MapEntry](c, value))
}

func (c FfiConverterMapEntry) Write(writer io.Writer, value MapEntry) {
	FfiConverterStringINSTANCE.Write(writer, value.Key)
	FfiConverterValueINSTANCE.Write(writer, value.Value)
}

type FfiDestroyerMapEntry struct{}

func (_ FfiDestroyerMapEntry) Destroy(value MapEntry) {
	value.Destroy()
}

type Row struct {
	Columns []string
	Values  []Value
}

func (r *Row) Destroy() {
	FfiDestroyerSequenceString{}.Destroy(r.Columns)
	FfiDestroyerSequenceValue{}.Destroy(r.Values)
}

type FfiConverterRow struct{}

var FfiConverterRowINSTANCE = FfiConverterRow{}

func (c FfiConverterRow) Lift(rb RustBufferI) Row {
	return LiftFromRustBuffer[Row](c, rb)
}

func (c FfiConverterRow) Read(reader io.Reader) Row {
	return Row{
		FfiConverterSequenceStringINSTANCE.Read(reader),
		FfiConverterSequenceValueINSTANCE.Read(reader),
	}
}

func (c FfiConverterRow) Lower(value Row) C.RustBuffer {
	return LowerIntoRustBuffer[Row](c, value)
}

func (c FfiConverterRow) LowerExternal(value Row) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Row](c, value))
}

func (c FfiConverterRow) Write(writer io.Writer, value Row) {
	FfiConverterSequenceStringINSTANCE.Write(writer, value.Columns)
	FfiConverterSequenceValueINSTANCE.Write(writer, value.Values)
}

type FfiDestroyerRow struct{}

func (_ FfiDestroyerRow) Destroy(value Row) {
	value.Destroy()
}

type SystemConfig struct {
	SessionDir                   *string
	DataDir                      *string
	MaxThreads                   *uint32
	S3AccessKeyId                *string
	S3SecretAccessKey            *string
	S3Region                     *string
	S3EndpointUrl                *string
	S3SessionToken               *string
	GcsAccessKeyId               *string
	GcsSecretAccessKey           *string
	AzureStorageAccountName      *string
	AzureStorageAccountKey       *string
	AzureStorageConnectionString *string
}

func (r *SystemConfig) Destroy() {
	FfiDestroyerOptionalString{}.Destroy(r.SessionDir)
	FfiDestroyerOptionalString{}.Destroy(r.DataDir)
	FfiDestroyerOptionalUint32{}.Destroy(r.MaxThreads)
	FfiDestroyerOptionalString{}.Destroy(r.S3AccessKeyId)
	FfiDestroyerOptionalString{}.Destroy(r.S3SecretAccessKey)
	FfiDestroyerOptionalString{}.Destroy(r.S3Region)
	FfiDestroyerOptionalString{}.Destroy(r.S3EndpointUrl)
	FfiDestroyerOptionalString{}.Destroy(r.S3SessionToken)
	FfiDestroyerOptionalString{}.Destroy(r.GcsAccessKeyId)
	FfiDestroyerOptionalString{}.Destroy(r.GcsSecretAccessKey)
	FfiDestroyerOptionalString{}.Destroy(r.AzureStorageAccountName)
	FfiDestroyerOptionalString{}.Destroy(r.AzureStorageAccountKey)
	FfiDestroyerOptionalString{}.Destroy(r.AzureStorageConnectionString)
}

type FfiConverterSystemConfig struct{}

var FfiConverterSystemConfigINSTANCE = FfiConverterSystemConfig{}

func (c FfiConverterSystemConfig) Lift(rb RustBufferI) SystemConfig {
	return LiftFromRustBuffer[SystemConfig](c, rb)
}

func (c FfiConverterSystemConfig) Read(reader io.Reader) SystemConfig {
	return SystemConfig{
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalUint32INSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterSystemConfig) Lower(value SystemConfig) C.RustBuffer {
	return LowerIntoRustBuffer[SystemConfig](c, value)
}

func (c FfiConverterSystemConfig) LowerExternal(value SystemConfig) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[SystemConfig](c, value))
}

func (c FfiConverterSystemConfig) Write(writer io.Writer, value SystemConfig) {
	FfiConverterOptionalStringINSTANCE.Write(writer, value.SessionDir)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.DataDir)
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.MaxThreads)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.S3AccessKeyId)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.S3SecretAccessKey)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.S3Region)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.S3EndpointUrl)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.S3SessionToken)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.GcsAccessKeyId)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.GcsSecretAccessKey)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.AzureStorageAccountName)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.AzureStorageAccountKey)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.AzureStorageConnectionString)
}

type FfiDestroyerSystemConfig struct{}

func (_ FfiDestroyerSystemConfig) Destroy(value SystemConfig) {
	value.Destroy()
}

type ClickGraphError struct {
	err error
}

// Convience method to turn *ClickGraphError into error
// Avoiding treating nil pointer as non nil error interface
func (err *ClickGraphError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err ClickGraphError) Error() string {
	return fmt.Sprintf("ClickGraphError: %s", err.err.Error())
}

func (err ClickGraphError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrClickGraphErrorDatabaseError = fmt.Errorf("ClickGraphErrorDatabaseError")
var ErrClickGraphErrorQueryError = fmt.Errorf("ClickGraphErrorQueryError")
var ErrClickGraphErrorExportError = fmt.Errorf("ClickGraphErrorExportError")

// Variant structs
type ClickGraphErrorDatabaseError struct {
	Msg string
}

func NewClickGraphErrorDatabaseError(
	msg string,
) *ClickGraphError {
	return &ClickGraphError{err: &ClickGraphErrorDatabaseError{
		Msg: msg}}
}

func (e ClickGraphErrorDatabaseError) destroy() {
	FfiDestroyerString{}.Destroy(e.Msg)
}

func (err ClickGraphErrorDatabaseError) Error() string {
	return fmt.Sprint("DatabaseError",
		": ",

		"Msg=",
		err.Msg,
	)
}

func (self ClickGraphErrorDatabaseError) Is(target error) bool {
	return target == ErrClickGraphErrorDatabaseError
}

type ClickGraphErrorQueryError struct {
	Msg string
}

func NewClickGraphErrorQueryError(
	msg string,
) *ClickGraphError {
	return &ClickGraphError{err: &ClickGraphErrorQueryError{
		Msg: msg}}
}

func (e ClickGraphErrorQueryError) destroy() {
	FfiDestroyerString{}.Destroy(e.Msg)
}

func (err ClickGraphErrorQueryError) Error() string {
	return fmt.Sprint("QueryError",
		": ",

		"Msg=",
		err.Msg,
	)
}

func (self ClickGraphErrorQueryError) Is(target error) bool {
	return target == ErrClickGraphErrorQueryError
}

type ClickGraphErrorExportError struct {
	Msg string
}

func NewClickGraphErrorExportError(
	msg string,
) *ClickGraphError {
	return &ClickGraphError{err: &ClickGraphErrorExportError{
		Msg: msg}}
}

func (e ClickGraphErrorExportError) destroy() {
	FfiDestroyerString{}.Destroy(e.Msg)
}

func (err ClickGraphErrorExportError) Error() string {
	return fmt.Sprint("ExportError",
		": ",

		"Msg=",
		err.Msg,
	)
}

func (self ClickGraphErrorExportError) Is(target error) bool {
	return target == ErrClickGraphErrorExportError
}

type FfiConverterClickGraphError struct{}

var FfiConverterClickGraphErrorINSTANCE = FfiConverterClickGraphError{}

func (c FfiConverterClickGraphError) Lift(eb RustBufferI) *ClickGraphError {
	return LiftFromRustBuffer[*ClickGraphError](c, eb)
}

func (c FfiConverterClickGraphError) Lower(value *ClickGraphError) C.RustBuffer {
	return LowerIntoRustBuffer[*ClickGraphError](c, value)
}

func (c FfiConverterClickGraphError) LowerExternal(value *ClickGraphError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*ClickGraphError](c, value))
}

func (c FfiConverterClickGraphError) Read(reader io.Reader) *ClickGraphError {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &ClickGraphError{&ClickGraphErrorDatabaseError{
			Msg: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 2:
		return &ClickGraphError{&ClickGraphErrorQueryError{
			Msg: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 3:
		return &ClickGraphError{&ClickGraphErrorExportError{
			Msg: FfiConverterStringINSTANCE.Read(reader),
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterClickGraphError.Read()", errorID))
	}
}

func (c FfiConverterClickGraphError) Write(writer io.Writer, value *ClickGraphError) {
	switch variantValue := value.err.(type) {
	case *ClickGraphErrorDatabaseError:
		writeInt32(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Msg)
	case *ClickGraphErrorQueryError:
		writeInt32(writer, 2)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Msg)
	case *ClickGraphErrorExportError:
		writeInt32(writer, 3)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Msg)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterClickGraphError.Write", value))
	}
}

type FfiDestroyerClickGraphError struct{}

func (_ FfiDestroyerClickGraphError) Destroy(value *ClickGraphError) {
	switch variantValue := value.err.(type) {
	case ClickGraphErrorDatabaseError:
		variantValue.destroy()
	case ClickGraphErrorQueryError:
		variantValue.destroy()
	case ClickGraphErrorExportError:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerClickGraphError.Destroy", value))
	}
}

type Value interface {
	Destroy()
}
type ValueNull struct {
}

func (e ValueNull) Destroy() {
}

type ValueBool struct {
	V bool
}

func (e ValueBool) Destroy() {
	FfiDestroyerBool{}.Destroy(e.V)
}

type ValueInt64 struct {
	V int64
}

func (e ValueInt64) Destroy() {
	FfiDestroyerInt64{}.Destroy(e.V)
}

type ValueFloat64 struct {
	V float64
}

func (e ValueFloat64) Destroy() {
	FfiDestroyerFloat64{}.Destroy(e.V)
}

type ValueString struct {
	V string
}

func (e ValueString) Destroy() {
	FfiDestroyerString{}.Destroy(e.V)
}

type ValueList struct {
	Items []Value
}

func (e ValueList) Destroy() {
	FfiDestroyerSequenceValue{}.Destroy(e.Items)
}

type ValueMap struct {
	Entries []MapEntry
}

func (e ValueMap) Destroy() {
	FfiDestroyerSequenceMapEntry{}.Destroy(e.Entries)
}

type FfiConverterValue struct{}

var FfiConverterValueINSTANCE = FfiConverterValue{}

func (c FfiConverterValue) Lift(rb RustBufferI) Value {
	return LiftFromRustBuffer[Value](c, rb)
}

func (c FfiConverterValue) Lower(value Value) C.RustBuffer {
	return LowerIntoRustBuffer[Value](c, value)
}

func (c FfiConverterValue) LowerExternal(value Value) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Value](c, value))
}
func (FfiConverterValue) Read(reader io.Reader) Value {
	id := readInt32(reader)
	switch id {
	case 1:
		return ValueNull{}
	case 2:
		return ValueBool{
			FfiConverterBoolINSTANCE.Read(reader),
		}
	case 3:
		return ValueInt64{
			FfiConverterInt64INSTANCE.Read(reader),
		}
	case 4:
		return ValueFloat64{
			FfiConverterFloat64INSTANCE.Read(reader),
		}
	case 5:
		return ValueString{
			FfiConverterStringINSTANCE.Read(reader),
		}
	case 6:
		return ValueList{
			FfiConverterSequenceValueINSTANCE.Read(reader),
		}
	case 7:
		return ValueMap{
			FfiConverterSequenceMapEntryINSTANCE.Read(reader),
		}
	default:
		panic(fmt.Sprintf("invalid enum value %v in FfiConverterValue.Read()", id))
	}
}

func (FfiConverterValue) Write(writer io.Writer, value Value) {
	switch variant_value := value.(type) {
	case ValueNull:
		writeInt32(writer, 1)
	case ValueBool:
		writeInt32(writer, 2)
		FfiConverterBoolINSTANCE.Write(writer, variant_value.V)
	case ValueInt64:
		writeInt32(writer, 3)
		FfiConverterInt64INSTANCE.Write(writer, variant_value.V)
	case ValueFloat64:
		writeInt32(writer, 4)
		FfiConverterFloat64INSTANCE.Write(writer, variant_value.V)
	case ValueString:
		writeInt32(writer, 5)
		FfiConverterStringINSTANCE.Write(writer, variant_value.V)
	case ValueList:
		writeInt32(writer, 6)
		FfiConverterSequenceValueINSTANCE.Write(writer, variant_value.Items)
	case ValueMap:
		writeInt32(writer, 7)
		FfiConverterSequenceMapEntryINSTANCE.Write(writer, variant_value.Entries)
	default:
		_ = variant_value
		panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterValue.Write", value))
	}
}

type FfiDestroyerValue struct{}

func (_ FfiDestroyerValue) Destroy(value Value) {
	value.Destroy()
}

type FfiConverterOptionalUint32 struct{}

var FfiConverterOptionalUint32INSTANCE = FfiConverterOptionalUint32{}

func (c FfiConverterOptionalUint32) Lift(rb RustBufferI) *uint32 {
	return LiftFromRustBuffer[*uint32](c, rb)
}

func (_ FfiConverterOptionalUint32) Read(reader io.Reader) *uint32 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint32INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint32) Lower(value *uint32) C.RustBuffer {
	return LowerIntoRustBuffer[*uint32](c, value)
}

func (c FfiConverterOptionalUint32) LowerExternal(value *uint32) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint32](c, value))
}

func (_ FfiConverterOptionalUint32) Write(writer io.Writer, value *uint32) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint32INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint32 struct{}

func (_ FfiDestroyerOptionalUint32) Destroy(value *uint32) {
	if value != nil {
		FfiDestroyerUint32{}.Destroy(*value)
	}
}

type FfiConverterOptionalString struct{}

var FfiConverterOptionalStringINSTANCE = FfiConverterOptionalString{}

func (c FfiConverterOptionalString) Lift(rb RustBufferI) *string {
	return LiftFromRustBuffer[*string](c, rb)
}

func (_ FfiConverterOptionalString) Read(reader io.Reader) *string {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterStringINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalString) Lower(value *string) C.RustBuffer {
	return LowerIntoRustBuffer[*string](c, value)
}

func (c FfiConverterOptionalString) LowerExternal(value *string) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*string](c, value))
}

func (_ FfiConverterOptionalString) Write(writer io.Writer, value *string) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalString struct{}

func (_ FfiDestroyerOptionalString) Destroy(value *string) {
	if value != nil {
		FfiDestroyerString{}.Destroy(*value)
	}
}

type FfiConverterOptionalRow struct{}

var FfiConverterOptionalRowINSTANCE = FfiConverterOptionalRow{}

func (c FfiConverterOptionalRow) Lift(rb RustBufferI) *Row {
	return LiftFromRustBuffer[*Row](c, rb)
}

func (_ FfiConverterOptionalRow) Read(reader io.Reader) *Row {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterRowINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalRow) Lower(value *Row) C.RustBuffer {
	return LowerIntoRustBuffer[*Row](c, value)
}

func (c FfiConverterOptionalRow) LowerExternal(value *Row) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*Row](c, value))
}

func (_ FfiConverterOptionalRow) Write(writer io.Writer, value *Row) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterRowINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalRow struct{}

func (_ FfiDestroyerOptionalRow) Destroy(value *Row) {
	if value != nil {
		FfiDestroyerRow{}.Destroy(*value)
	}
}

type FfiConverterSequenceString struct{}

var FfiConverterSequenceStringINSTANCE = FfiConverterSequenceString{}

func (c FfiConverterSequenceString) Lift(rb RustBufferI) []string {
	return LiftFromRustBuffer[[]string](c, rb)
}

func (c FfiConverterSequenceString) Read(reader io.Reader) []string {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]string, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterStringINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceString) Lower(value []string) C.RustBuffer {
	return LowerIntoRustBuffer[[]string](c, value)
}

func (c FfiConverterSequenceString) LowerExternal(value []string) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]string](c, value))
}

func (c FfiConverterSequenceString) Write(writer io.Writer, value []string) {
	if len(value) > math.MaxInt32 {
		panic("[]string is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterStringINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceString struct{}

func (FfiDestroyerSequenceString) Destroy(sequence []string) {
	for _, value := range sequence {
		FfiDestroyerString{}.Destroy(value)
	}
}

type FfiConverterSequenceMapEntry struct{}

var FfiConverterSequenceMapEntryINSTANCE = FfiConverterSequenceMapEntry{}

func (c FfiConverterSequenceMapEntry) Lift(rb RustBufferI) []MapEntry {
	return LiftFromRustBuffer[[]MapEntry](c, rb)
}

func (c FfiConverterSequenceMapEntry) Read(reader io.Reader) []MapEntry {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]MapEntry, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterMapEntryINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceMapEntry) Lower(value []MapEntry) C.RustBuffer {
	return LowerIntoRustBuffer[[]MapEntry](c, value)
}

func (c FfiConverterSequenceMapEntry) LowerExternal(value []MapEntry) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]MapEntry](c, value))
}

func (c FfiConverterSequenceMapEntry) Write(writer io.Writer, value []MapEntry) {
	if len(value) > math.MaxInt32 {
		panic("[]MapEntry is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterMapEntryINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceMapEntry struct{}

func (FfiDestroyerSequenceMapEntry) Destroy(sequence []MapEntry) {
	for _, value := range sequence {
		FfiDestroyerMapEntry{}.Destroy(value)
	}
}

type FfiConverterSequenceRow struct{}

var FfiConverterSequenceRowINSTANCE = FfiConverterSequenceRow{}

func (c FfiConverterSequenceRow) Lift(rb RustBufferI) []Row {
	return LiftFromRustBuffer[[]Row](c, rb)
}

func (c FfiConverterSequenceRow) Read(reader io.Reader) []Row {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]Row, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterRowINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceRow) Lower(value []Row) C.RustBuffer {
	return LowerIntoRustBuffer[[]Row](c, value)
}

func (c FfiConverterSequenceRow) LowerExternal(value []Row) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]Row](c, value))
}

func (c FfiConverterSequenceRow) Write(writer io.Writer, value []Row) {
	if len(value) > math.MaxInt32 {
		panic("[]Row is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterRowINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceRow struct{}

func (FfiDestroyerSequenceRow) Destroy(sequence []Row) {
	for _, value := range sequence {
		FfiDestroyerRow{}.Destroy(value)
	}
}

type FfiConverterSequenceValue struct{}

var FfiConverterSequenceValueINSTANCE = FfiConverterSequenceValue{}

func (c FfiConverterSequenceValue) Lift(rb RustBufferI) []Value {
	return LiftFromRustBuffer[[]Value](c, rb)
}

func (c FfiConverterSequenceValue) Read(reader io.Reader) []Value {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]Value, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterValueINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceValue) Lower(value []Value) C.RustBuffer {
	return LowerIntoRustBuffer[[]Value](c, value)
}

func (c FfiConverterSequenceValue) LowerExternal(value []Value) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]Value](c, value))
}

func (c FfiConverterSequenceValue) Write(writer io.Writer, value []Value) {
	if len(value) > math.MaxInt32 {
		panic("[]Value is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterValueINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceValue struct{}

func (FfiDestroyerSequenceValue) Destroy(sequence []Value) {
	for _, value := range sequence {
		FfiDestroyerValue{}.Destroy(value)
	}
}
