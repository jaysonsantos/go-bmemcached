package bmemcached

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type (
	// Connection is a struct that hold a direct connection to memcached
	Connection struct {
		conn net.Conn
	}

	Request struct {
		magic        uint8
		opcode       uint8
		keyLength    uint16
		extrasLength uint8
		dataType     uint8
		reserved     uint16
		bodyLength   uint32
		opaque       uint32
		cas          uint64
	}

	Response struct {
		magic        uint8
		opcode       uint8
		keyLength    uint16
		extrasLength uint8
		dataType     uint8
		status       uint16
		bodyLength   uint32
		opaque       uint32
		cas          uint64
	}
)

const (
	HeaderSize   = 24
	TypeRequest  = 0x80
	TypeResponse = 0x81

	CommandGet    = 0x00
	CommandSet    = 0x01
	CommandDelete = 0x04

	StatusSuccess        = 0x00
	StatusKeyNotFound    = 0x01
	StatusKeyExists      = 0x02
	StatusUnknownCommand = 0x81
)

// New creates a Connection to memcached
func New(addr string) (Connection, error) {
	var conn Connection
	connection, err := net.Dial("tcp", addr)
	if err == nil {
		conn = Connection{connection}
	} else {
		fmt.Println(err)
	}
	return conn, err
}

func (c *Connection) writeRequest(request Request, finalPayload []byte) error {
	var tempBuffer, // Used to convert bigger integers to []byte
		buf []byte
	writer := bytes.NewBuffer(buf)
	writer.WriteByte(request.magic)
	writer.WriteByte(request.opcode)

	tempBuffer = make([]byte, 2)
	binary.BigEndian.PutUint16(tempBuffer, request.keyLength)
	writer.Write(tempBuffer)

	writer.WriteByte(request.extrasLength)
	writer.WriteByte(request.dataType)

	tempBuffer = make([]byte, 2)
	binary.BigEndian.PutUint16(tempBuffer, request.reserved)
	writer.Write(tempBuffer)

	tempBuffer = make([]byte, 4)
	binary.BigEndian.PutUint32(tempBuffer, request.bodyLength)
	writer.Write(tempBuffer)

	tempBuffer = make([]byte, 4)
	binary.BigEndian.PutUint32(tempBuffer, request.opaque)
	writer.Write(tempBuffer)

	tempBuffer = make([]byte, 8)
	binary.BigEndian.PutUint64(tempBuffer, request.cas)
	writer.Write(tempBuffer)
	_, err := c.conn.Write(writer.Bytes())

	if err != nil {
		return err
	}
	_, err = c.conn.Write(finalPayload)
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) parseResponse(buf []byte) (Response, error) {
	var magic, opcode, extrasLength, dataType uint8
	var keyLength, reserved uint16
	var bodyLength, opaque uint32
	var cas uint64
	var err error
	var response Response

	reader := bytes.NewBuffer(buf)
	magic, err = reader.ReadByte()
	if err != nil {
		return response, err
	}
	opcode, err = reader.ReadByte()
	if err != nil {
		return response, err
	}
	err = binary.Read(reader, binary.BigEndian, &keyLength)
	if err != nil {
		return response, err
	}
	extrasLength, err = reader.ReadByte()
	if err != nil {
		return response, err
	}
	dataType, err = reader.ReadByte()
	if err != nil {
		return response, err
	}
	err = binary.Read(reader, binary.BigEndian, &reserved)
	if err != nil {
		return response, err
	}
	err = binary.Read(reader, binary.BigEndian, &bodyLength)
	if err != nil {
		return response, err
	}
	err = binary.Read(reader, binary.BigEndian, &opaque)
	if err != nil {
		return response, err
	}
	err = binary.Read(reader, binary.BigEndian, &cas)
	if err != nil {
		return response, err
	}

	response = Response{magic, opcode, keyLength, extrasLength, dataType,
		reserved, bodyLength, opaque, cas}

	return response, nil
}

func (c *Connection) readResponse() (Response, error) {
	buf := make([]byte, HeaderSize)
	c.conn.Read(buf)
	response, err := c.parseResponse(buf)
	if err != nil {
		return response, err
	}

	return response, nil
}

// Set is used to set a value on memcached
func (c *Connection) Set(key string, value string, expirationTime uint32) (int, error) {
	var tempBuffer, finalPayload []byte
	var err error

	extrasLength := 8 // Flags and Expiration Time
	bodyLength := uint32(len(key) + len(value) + extrasLength)
	request := Request{TypeRequest, CommandSet, uint16(len(key)), uint8(extrasLength), 0x00, 0x00,
		bodyLength, 0x00, 0x00}

	writer := bytes.NewBuffer(finalPayload)

	tempBuffer = make([]byte, 4)
	binary.BigEndian.PutUint32(tempBuffer, 0x00) // Flags
	writer.Write(tempBuffer)

	tempBuffer = make([]byte, 4)
	binary.BigEndian.PutUint32(tempBuffer, expirationTime)
	writer.Write(tempBuffer)

	writer.WriteString(key)
	writer.WriteString(value)

	err = c.writeRequest(request, writer.Bytes())
	if err != nil {
		return 0, err
	}
	_, err = c.readResponse()

	if err != nil {
		return 0, err
	}

	return len(value), err
}

// Delete a key
func (c *Connection) Delete(key string) error {
	request := Request{TypeRequest, CommandDelete, uint16(len(key)), 0x00, 0x00, 0x00,
		uint32(len(key)), 0x00, 0x00}

	finalPayload := bytes.NewBuffer(make([]byte, 0))
	finalPayload.WriteString(key)

	err := c.writeRequest(request, finalPayload.Bytes())
	if err != nil {
		return err
	}
	_, err = c.readResponse()

	return err
}
