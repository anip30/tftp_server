/**********************************************************
Project :	TFTP Server in GOlang
Author : Anipkumar Patel
Purpose:	TFTP server is used to server the tftp clients.
		This TFTP server is in memory server. It stores file in memory only not on disk.
		So when you stop/kill the server all files will be lost.
		TFTP protocol is simple protocol to transfer files.
		It has very limited functionality. Like get/put files only
***********************************************************/
package main

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	//opcodes
	RRQ   uint16 = 1
	WRQ   uint16 = 2
	DATA  uint16 = 3
	ACK   uint16 = 4
	ERROR uint16 = 5

	//errors
	UNKNOWNERROR    uint16 = 0
	FILENOTFOUND    uint16 = 1
	ACCESSVIOLATION uint16 = 2
	DISKFULL        uint16 = 3
	ILLEGALOP       uint16 = 4
	UNKNOWNID       uint16 = 5
	FILEEXISTS      uint16 = 6
	USERNOTFOUND    uint16 = 7

	FILEBLOCKSIZE uint16 = 512
	TIMEOUT              = 2

	//error message
	FILENOTFOUNDMSG string = "File not found"
	FILEEXISTSMSG   string = "File already exist"
)

// request structure
type RequestData struct {
	OPcode     uint16       //opcode
	FileName   string       // requested file name
	Mode       string       // Operating mode. We are handling only octet mode
	ClientAddr *net.UDPAddr //client address
}

//Map containing file name and its list of blocks. This is small part of file system implementation.
// It maps file name to its data blocks
var FileMap map[string]*list.List

/**
* @brief : Fucntion to Parse Request received from client.
* @param : buf: Raw data of request.
* @param : ReqLen: request length
* @param : ReqData: result of parsing
 */
func ParseRequest(buf []byte, ReqLen uint16, ReqData *RequestData) {

	ReqData.OPcode = binary.BigEndian.Uint16(buf[0:2]) //opcode
	pos := strings.IndexByte(string(buf[2:]), 0x00)
	ReqData.FileName = string(buf[2 : pos+2])    // extracting file name
	ReqData.Mode = string(buf[pos+3 : ReqLen-1]) // extracting operating mode.
}

/**
* @brief : Function to send ack packet to client
* @param : BlockNo : Block number to acknoledge
* @param : conn : client connection
 */

func SendACKPacket(BlockNo uint16, Conn *net.UDPConn) {

	var ack_data []byte = make([]byte, 4)
	offset := 0
	binary.BigEndian.PutUint16(ack_data[offset:], ACK) //setting OPCODE as ACK
	offset = offset + 2
	binary.BigEndian.PutUint16(ack_data[offset:], BlockNo) // setting BLOCK number Acknoledged
	_, err := Conn.Write(ack_data)                         //writing ACK packet to client
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
}

/**
* @brief : Function to send Error packet to client
* @param : ErrNo : Error Number
* @param : ErrStr : Error string associated with that error number
* @param : conn : client connection
 */

func SendErrorPacket(ErrNo uint16, ErrStr string, Conn *net.UDPConn) {

	fmt.Println("\n==== Error packet ===== ", ErrStr)
	var ErrPkt []byte = make([]byte, 5+len(ErrStr))
	offset := 0
	binary.BigEndian.PutUint16(ErrPkt[offset:], ERROR) //setting OPCODE as ERROR
	offset = offset + 2
	binary.BigEndian.PutUint16(ErrPkt[offset:], ErrNo) //setting Error no
	offset = offset + 2
	copy(ErrPkt[offset:], ErrStr) //setting error string
	offset = offset + len(ErrStr)
	ErrPkt[offset] = 0x00
	_, err := Conn.Write(ErrPkt) //writing Error packet to client
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
}

/**
* @brief : Function to handle Write Request. This will write data to main mamory not on disk.
* @param : ReqData: Request iformation
 */

func HandleWriteRequest(ReqData *RequestData) {

	var ACKNo uint16
	var FileBlocklist *list.List
	ACKNo = 0

	/*after intial request we will use different local port(TID) to do further data
	transfer so creating new address with different port and connecting to client.*/

	RequestAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	NewConn, err := net.DialUDP("udp", RequestAddr, ReqData.ClientAddr)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer NewConn.Close() //defering connection close to end of request handling.

	FileBlocklist = list.New()
	RetryCnt := 0
	if _, ok := FileMap[ReqData.FileName]; ok { //checking file already exists. if yes send error message
		SendErrorPacket(FILEEXISTS, FILEEXISTSMSG, NewConn)
		return
	} else {
		fmt.Println("\n==== Write Started for :[", ReqData.FileName, "]") // Sending first ACK to client
		SendACKPacket(ACKNo, NewConn)
	}

	ACKNo = ACKNo + 1
	TempBuf := make([]byte, FILEBLOCKSIZE+4)

	for {
		//setting read timeout
		NewConn.SetReadDeadline(time.Now().Add(TIMEOUT * time.Second))
		byte_read, _, err := NewConn.ReadFromUDP(TempBuf) //reading data to write from client
		if err != nil {
			TimeoutErr, Status := err.(net.Error)
			if Status && TimeoutErr.Timeout() { //if timeout occured then try again to read
				if RetryCnt >= 3 { // if retry count is reached to limit then return
					fmt.Println("\n==== TIMEOUT in Reading from client :[", ReqData.ClientAddr, "]")
					return
				}
				SendACKPacket(ACKNo-1, NewConn) //sending previous ack again while retrying may be it get lost.
				RetryCnt = RetryCnt + 1         // increment retry count
				continue
			}
			//if other error occured then send error message and discard this request
			SendErrorPacket(UNKNOWNERROR, string("Error not able to receive data at server from client"), NewConn)
			return
		}
		ReadBuf := make([]byte, byte_read-4)
		//		fmt.Println("byte read in writing", byte_read)
		offset := 0
		OPcode := binary.BigEndian.Uint16(TempBuf[offset:])
		offset = offset + 2
		BlockNo := binary.BigEndian.Uint16(TempBuf[offset:])
		offset = offset + 2
		//		fmt.Println("block received ", BlockNo)
		if OPcode == ERROR { // if opcode is error then stop this request and discard it
			fmt.Println("Error received from client")
			return
		}

		if BlockNo != ACKNo {
			fmt.Println("==== Out of order Data Packet received from client ")
			return
		}

		_ = copy(ReadBuf, TempBuf[offset:])

		//		byte_copied := copy(ReadBuf, TempBuf[offset:])
		//		fmt.Println("byte copied in writing", byte_copied)

		FileBlocklist.PushBack(ReadBuf) // add received block to list of block of given file
		//		fmt.Println("ACK for writing ", ACKNo)
		SendACKPacket(ACKNo, NewConn) //sending ACK for received block
		ACKNo = ACKNo + 1
		RetryCnt = 0
		if byte_read < 516 { //checking for last packet received
			break
		}
	}
	//adding file blocks list to file map. Adding it here so file will be only
	//visible after it is stored in map
	FileMap[ReqData.FileName] = FileBlocklist
	fmt.Println("\n==== Write Completed for :[", ReqData.FileName, "]")
	return
}

/**
* @brief : Function to handle Read Request. Read data from main memory and sent to client
* @param : ReqData: Request iformation
 */

func HandleReadRequest(ReqData *RequestData) {

	var FileBlocklist *list.List
	var ok bool

	/*after intial request we will use different local port(TID) to do further data
	transfer so creating new address with different port and connecting to client.*/

	RequestAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	NewConn, err := net.DialUDP("udp", RequestAddr, ReqData.ClientAddr)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer NewConn.Close() //defering connection close to end of request handling.

	if FileBlocklist, ok = FileMap[ReqData.FileName]; !ok { //checking for file availability.
		SendErrorPacket(FILENOTFOUND, FILENOTFOUNDMSG, NewConn) //if not exist send error message of "file not found"
		return
	}
	fmt.Println("\n==== Read Started for :[", ReqData.FileName, "]")
	DataToSend := make([]byte, FILEBLOCKSIZE+4)
	ACKRec := make([]byte, 1024)
	var BlockCount uint16 = 1 //block count for sending ACK
	RetryCnt := 0

	for e := FileBlocklist.Front(); e != nil; { //iterating over all files blocks in its list

		offset := 0
		binary.BigEndian.PutUint16(DataToSend[offset:], DATA) //setting opcode DATA in packet
		offset = offset + 2
		binary.BigEndian.PutUint16(DataToSend[offset:], BlockCount) //setting Block number in packet
		offset = offset + 2
		ByteCopied := copy(DataToSend[offset:], e.Value.([]byte)) // copying block data in packet

		//		fmt.Println("byte copied to send ", ByteCopied)

		_, err := NewConn.Write(DataToSend[:4+ByteCopied]) //writing data packet to client
		//		fmt.Println("byte copied to send ", byte_written)
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}
		//reading ACK for data sent above
		// Setting read deadine for timeout and trying to read for 3 attempt.
		NewConn.SetReadDeadline(time.Now().Add(TIMEOUT * time.Second))
		_, _, err = NewConn.ReadFromUDP(ACKRec)
		if err != nil {
			TimeoutErr, Status := err.(net.Error)
			if Status && TimeoutErr.Timeout() {
				if RetryCnt >= 3 { // if retry reach to thresold then stop and discard the reqeust.
					fmt.Println("\n==== TIMEOUT in Reading from client :[", ReqData.ClientAddr, "]")
					return
				}
				RetryCnt = RetryCnt + 1
				continue //trying again if not enough retry done
			}
			//  send error message to client. Unknown error
			SendErrorPacket(UNKNOWNERROR, string("Error not able to receive ACK at server from client"), NewConn)
			return
		}

		offset = 0
		OPcode := binary.BigEndian.Uint16(ACKRec[offset:])
		offset = offset + 2
		BlockNoFromACK := binary.BigEndian.Uint16(ACKRec[offset:])

		//		fmt.Println("Opcode ", OPcode, "block no ", BlockNoFromACK)

		if OPcode == ERROR { // If error received instead of ACK then stop this request and discard it
			fmt.Println("Error received from client")
			return
		}

		if OPcode == ACK && BlockNoFromACK == BlockCount { //if ack received for last packet sent then send next data block
			BlockCount = BlockCount + 1
			e = e.Next()
			RetryCnt = 0 //resetting retry count if ACK received successfully
		}
	}
	fmt.Println("\n==== Read Completed for :[", ReqData.FileName, "]")
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("\n==== Please enter command line argument := [ip address:port] \n")
		return
	}

	IpPort := strings.Split(os.Args[1], ":") //checking for port number it must be different than 59
	if IpPort[1] == "59" {
		fmt.Println("\n==== Please enter Port Number other than 59 \n")
		return
	}

	port, err := strconv.Atoi(IpPort[1]) //checking port number is in valid range
	if port < 1024 || port > 65536 {
		fmt.Println("\n==== Please enter Port Number in range [1024:65536] \n")
		return

	}

	ip := net.ParseIP(IpPort[0])

	if IpPort[0] != "" && ip == nil { //checking for validity for ip address
		fmt.Println("\n==== Please enter Valid Ip Adress \n")
		return
	}

	FileMap = make(map[string]*list.List) //setting filemap
	buf := make([]byte, 516)

	ServerAddr, err := net.ResolveUDPAddr("udp", os.Args[1]) //setting port on which tftp server listen for requests.
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}

	ServerConn, err := net.ListenUDP("udp", ServerAddr) //listening on given port for request
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	fmt.Println("\n==== server started at [", ServerAddr, "]")

	defer ServerConn.Close()

	for {
		n, addr, err := ServerConn.ReadFromUDP(buf) //read request from client
		//		fmt.Println("Received ", buf[0:n], " from ", addr)
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}
		Req := new(RequestData)
		ParseRequest(buf, uint16(n), Req) //parse the request
		Req.ClientAddr = addr

		if Req.OPcode == ERROR { // If error message received then do nothing
			fmt.Println(" Error received from client ")
			continue
		}
		if Req.OPcode == RRQ {
			fmt.Println("\n==== Read reqeust file : [", Req.FileName, "] & client : [", Req.ClientAddr, "]")
			go HandleReadRequest(Req)
		}
		if Req.OPcode == WRQ {
			fmt.Println("\n==== Write reqeust file : [", Req.FileName, "] from client : [", Req.ClientAddr, "]")
			go HandleWriteRequest(Req)
		}
	}
}
