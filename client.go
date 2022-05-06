package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)
const (
	serverIP = "127.0.0.1"
)

func getDatacenterPorts() []string{
	return []string{"8080","8081","8082"}
}

type Client struct{
	decoder *json.Decoder
	encoder *json.Encoder
	conn net.Conn
}

func NewClient(serverPort string) (*Client,error){
	conn, err := net.Dial("tcp", serverIP+":"+serverPort)
	if err != nil {
		fmt.Printf("Server is not online - %v\n", err)
		return nil,err
	}
	fmt.Println("running on",conn.LocalAddr().String(),conn.RemoteAddr().String())
	return &Client{
		conn:conn,
		decoder: json.NewDecoder(conn),
		encoder: json.NewEncoder(conn),
	},nil
}

func displayHelpPrompt() {
	fmt.Print("Causal Consistency Lab\nUse the following commands:\n 1) write 'key' 'value'\n 2) read 'key'\n")
}

func printRepl() {
	fmt.Print("> ")
}
func stripInput(txt string) string {
	output := strings.TrimSpace(txt)
	output = strings.ToLower(output)
	return output
}
type MessageType int
const (
	ClientWrite MessageType = iota
	ClientRead
	ReplicatedWrite
)
type Request struct{
	RType MessageType `json:"type"`
	Key string `json:"key"`
	Value string `json:"value,omitempty"`
}

type Response struct{
	RType MessageType `json:"type"`
	Key string `json:"key"`
	Value string `json:"value,omitempty"`
	Status string `json:"status"`
}
func main(){
	if len(os.Args)<2{
		fmt.Println("Missing datacenter number")
		return
	}
	val,_ := strconv.Atoi(os.Args[1])
	client,_ := NewClient(getDatacenterPorts()[val])
	reader := bufio.NewScanner(os.Stdin)
	displayHelpPrompt()
	printRepl()
	for reader.Scan() {
		text := strings.Fields(stripInput(reader.Text()))
		switch text[0] {
		case "read":
			client.sendReadReq(text[1])
		case "write":
			client.sendWriteReq(text[1],text[2])
		}
		printRepl()
	}
}

func (c *Client) sendReadReq(key string){
	request := Request{
		Key: key,
		RType: ClientRead,
	}
	if err := c.encoder.Encode(request); err != nil {
		fmt.Println("Encode error: ", err)
	}
	var response Response
	if err:= c.decoder.Decode(&response); err!=nil{
		fmt.Println("Server response decode error",err)
	}
	if response.Status=="ok"{
		fmt.Println("Read result:",response.Value)
	} else{
		fmt.Println("Key error")
	}
}

func (c *Client) sendWriteReq(key string, value string){
	request := Request{
		Key: key,
		RType: ClientWrite,
		Value: value,
	}
	if err := c.encoder.Encode(request); err != nil {
		fmt.Println("Encode error: ", err)
	}
	fmt.Println("sent write req",request)
	var response Response
	if err:= c.decoder.Decode(&response); err!=nil{
		fmt.Println("Server response decode error",err)
	}
	if response.Status=="ok"{
		fmt.Println("Wrote value",value,"to key:",key)
	}
}