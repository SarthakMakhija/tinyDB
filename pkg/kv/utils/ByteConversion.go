package utils

import (
	"encoding/binary"
	"reflect"
	"unsafe"
)

const uint32Size = int(unsafe.Sizeof(uint32(0)))

func U32SliceToBytes(uint32s []uint32) []byte {
	if len(uint32s) == 0 {
		return nil
	}
	var bytes []byte
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	sliceHeader.Len = len(uint32s) * uint32Size
	sliceHeader.Cap = sliceHeader.Len
	sliceHeader.Data = uintptr(unsafe.Pointer(&uint32s[0]))
	return bytes
}

func U32ToBytesLittleEndian(value uint32) []byte {
	var bytes [uint32Size]byte
	binary.LittleEndian.PutUint32(bytes[:], value)
	return bytes[:]
}
