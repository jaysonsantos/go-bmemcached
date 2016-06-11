package bmemcached

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

type (
	// Connection is a struct that hold a direct connection to memcached
	Connection struct {
		conn net.Conn
	}

	_Request struct {
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

	_Response struct {
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
	headerSize   = 24
	typeRequest  = 0x80
	typeResponse = 0x81

	commandGet    = 0x00
	commandSet    = 0x01
	commandDelete = 0x04

	statusSuccess        = 0x00
	statusKeyNotFound    = 0x01
	statusKeyExists      = 0x02
	statusUnknownCommand = 0x81
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

func (c *Connection) writeRequest(request _Request, finalPayload []byte) error {
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

func (c *Connection) parseResponse(buf []byte) (_Response, error) {
	var magic, opcode, extrasLength, dataType uint8
	var keyLength, reserved uint16
	var bodyLength, opaque uint32
	var cas uint64
	var err error
	var response _Response

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

	response = _Response{magic, opcode, keyLength, extrasLength, dataType,
		reserved, bodyLength, opaque, cas}

	return response, nil
}

func (c *Connection) getErrorMessage(errorCode uint16) string {
	switch errorCode {
	case statusKeyExists:
		return "Key already exists"
	case statusKeyNotFound:
		return "Key not found"
	case statusUnknownCommand:
		return "Unkown command"
	default:
		return fmt.Sprintf("Unkown error code '%d'", errorCode)
	}
}

func (c *Connection) readResponse() (_Response, error) {
	var response _Response
	buf := make([]byte, headerSize)
	c.conn.Read(buf)
	if buf[0] != typeRequest && buf[0] != typeResponse {
		return response, fmt.Errorf("Server sent an unknown code: %d", buf[0])
	}
	response, err := c.parseResponse(buf)
	if err != nil {
		return response, err
	}

	switch response.status {
	case statusSuccess:
		break
	case statusKeyExists, statusKeyNotFound, statusUnknownCommand:
		errorMessage := c.getErrorMessage(response.status)
		c.conn.Read(make([]byte, response.bodyLength))
		return response, errors.New(errorMessage)
	}

	return response, nil
}

// Set a value on memcached
func (c *Connection) Set(key string, value string, expirationTime uint32) (int, error) {
	var tempBuffer, finalPayload []byte
	var err error

	extrasLength := 8 // Flags and Expiration Time
	bodyLength := uint32(len(key) + len(value) + extrasLength)
	request := _Request{typeRequest, commandSet, uint16(len(key)), uint8(extrasLength), 0x00, 0x00,
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

// Get a key on memcached
func (c *Connection) Get(key string) (string, error) {
	var tempBuffer []byte
	// TODO: Add cas
	request := _Request{typeRequest, commandGet, uint16(len(key)), 0x00, 0x00, 0x00,
		uint32(len(key)), 0x00, 0x00}

	finalPayload := bytes.NewBuffer(make([]byte, 0))
	finalPayload.WriteString(key)

	err := c.writeRequest(request, finalPayload.Bytes())
	if err != nil {
		return "", err
	}
	response, err := c.readResponse()
	if err != nil {
		return "", err
	}
	// TODO: Treat flags
	tempBuffer = make([]byte, 4)
	c.conn.Read(tempBuffer)

	tempBuffer = make([]byte, response.bodyLength-uint32(response.extrasLength))
	c.conn.Read(tempBuffer)

	return string(tempBuffer), nil
}

// Delete a key
func (c *Connection) Delete(key string) error {
	request := _Request{typeRequest, commandDelete, uint16(len(key)), 0x00, 0x00, 0x00,
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
