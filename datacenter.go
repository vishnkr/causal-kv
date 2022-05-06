package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	connIP   = "127.0.0.1"
	connType = "tcp"
)

func getDatacenterPorts() map[int]int{
	return map[int]int{8080:0,8081:1,8082:2}
}
type Version struct{
	Timestamp int `json:"timestamp"`
	DatacenterId int `json:"datacenter_id"`
}
type MessageType int
const (
	ClientWrite MessageType = iota
	ClientRead
	ReplicatedWrite
)
type Store struct{
	id int
	clients map[string]*Client
	port int
	data map[string]string
	localTime int
	pendingMessages []Msg
	commitedDependencies []Dependency
}
type Dependency struct {
	Key string `json:"key"`
	Version Version `json:"version"`
}

type Msg struct{
	RType MessageType `json:"type"`
	Key string `json:"key"`
	Value string `json:"value,omitempty"`
	Timestamp int `json:"timestamp,omitempty"`
	DatacenterId int `json:"datacenter_id,omitempty"`
	Dependencies []Dependency `json:"dependencies,omitempty"`
	Status string `json:"status,omitempty"`
}

type Client struct{
	id int
	dependency map[string]Dependency
	conn     net.Conn
	decoder *json.Decoder
	encoder *json.Encoder
}


var dataCenter Store

func getPort(addr string) int{
	split:= strings.Split(addr,":")
	port,_ := strconv.Atoi(split[1])
	return port
}
func (d *Store) handleConnection(conn net.Conn){
	var msg Msg
	for {
		decoder:= json.NewDecoder(conn)
		if err:= decoder.Decode(&msg); err!=nil{
			//fmt.Println("Request decode error",err)
			return
		}
		if msg.RType == ReplicatedWrite{
			fmt.Println("SERVER message")
			d.receiveReplicatedWrite(msg)
		} else{
			fmt.Println("CLIENT message")
			d.handleClient(conn,msg)
		}
	}
}

func (d *Store) handleClient(conn net.Conn,request Msg){
	clientAddr:= conn.RemoteAddr().String()
	if _,ok:=d.clients[clientAddr]; !ok{
		//client is not already connected
		d.clients[clientAddr] = &Client{
			id:len(d.clients)+1,
			dependency: make(map[string]Dependency,0),
			decoder: json.NewDecoder(conn),
			encoder: json.NewEncoder(conn),
			conn: conn,
			} 
	}
	//client write
	if request.RType==ClientWrite{
		d.data[request.Key]=request.Value
		d.localTime+=1
		fmt.Println("updated local time to",d.localTime)
		newVersion := Version{Timestamp: d.localTime,DatacenterId: d.id}
		newDep:=Dependency{Key:request.Key,Version: newVersion }
		d.commitedDependencies = append(d.commitedDependencies, newDep)
		fmt.Println("client write successful")
		depList:=make([]Dependency,0)
		for _,v := range(d.clients[clientAddr].dependency){
			depList = append(depList, v)
		}
		
		msg:= Msg{
			RType: ReplicatedWrite,
			Key: request.Key,
			Value: request.Value,
			Timestamp: d.localTime,
			DatacenterId: d.id,
			Dependencies: depList,
		}
		for port,id := range(getDatacenterPorts()){
			if id==d.id{
				continue
			}
			//delay replicated write if key is y for datacenter 2
			maxDelay:=20
			minDelay:=10
			n := rand.Intn(maxDelay-minDelay + 1)+minDelay
			if request.Key=="y" && id==2{
				fmt.Println("delaying replicated write for",n,"seconds")
				
				go func(){
					time.Sleep(time.Duration(n)*time.Second)
					d.sendReplicatedWrite(port,msg)
					fmt.Println("resume replicated write")
				}()
				
			} else{
				d.sendReplicatedWrite(port,msg)
			}
			
		}
		d.clients[clientAddr].encoder.Encode(Msg{Key: request.Key,Value: request.Value,Status:"ok"})
		//update dependency
		d.updateDependency(clientAddr,request.Key,ClientWrite)
	} else{
		//client read
		value,ok:= d.data[request.Key]
		if !ok{
			d.clients[clientAddr].encoder.Encode(Msg{Key: request.Key,Value:value,Status:"err"})
		} else{
			d.clients[clientAddr].encoder.Encode(Msg{Key: request.Key,Value:value,Status:"ok"})
		}
		d.updateDependency(clientAddr,request.Key,ClientRead)
		
	}
}

func delayReplicatedWrite(seconds int, sendWrite func()){
	time.Sleep(time.Duration(seconds) * time.Second)
}

func (d *Store)displayDependencies(){
	fmt.Println("---------------------------")
	for client:=range(d.clients){
		fmt.Println("Client client has dependency:",d.clients[client].dependency)
	}
	fmt.Println("-----------------------------")
}
func (d *Store) updateDependency(clientAddr string, key string, Rtype MessageType){
	//d.displayDependencies()
	newVersion:= Version{
		Timestamp: d.localTime,
		DatacenterId: d.id,
	}

	if Rtype == ClientWrite{
		d.clients[clientAddr].dependency[key] = Dependency{}
	} else {
		for i := len(d.commitedDependencies)-1; i>=0; i--{
			if d.commitedDependencies[i].Key == key{
				newVersion.Timestamp = d.commitedDependencies[i].Version.Timestamp
				newVersion.DatacenterId = d.commitedDependencies[i].Version.DatacenterId
				break
			}
		}
	}
	d.clients[clientAddr].dependency[key] = Dependency{Key:key,Version:newVersion}
	fmt.Println("Updated client dependency:",d.clients[clientAddr].dependency)
}

func (d *Store) handleServer(conn net.Conn,msg Msg){
	fmt.Println("received replicated request")
	d.receiveReplicatedWrite(msg)
}



func (d *Store) sendReplicatedWrite(port int, msg Msg){//key string, value string, clientAddr string, datacenterId int,time int){
	var serverAddr string = connIP + ":" + strconv.Itoa(port)
	conn, _ := net.Dial("tcp", serverAddr)
	encoder:= json.NewEncoder(conn)
	if err:=encoder.Encode(msg);err!=nil{
		fmt.Println("replicated write encode error",err)
	}
	fmt.Println("send replicated write to",serverAddr,"from",d.port,"message:",msg)
}

func (d *Store) receiveReplicatedWrite(msg Msg){
	fmt.Println("received msg",msg)
	if d.dependencyCheck(msg.Dependencies){
		d.commitMessage(msg)
		stillPending := []Msg{}
		for _,pendingMsg := range (d.pendingMessages){
			if d.dependencyCheck(pendingMsg.Dependencies){
				d.commitMessage(pendingMsg)
			} else {
				stillPending = append(stillPending, pendingMsg)
			}
		}
		d.pendingMessages = stillPending
	} else{
		d.pendingMessages = append(d.pendingMessages,msg)
	}
}

func (d* Store) commitMessage(msg Msg){
	d.data[msg.Key] = msg.Value
	if msg.Timestamp > d.localTime + 1{
		d.localTime = msg.Timestamp
	} else{
		d.localTime += 1 
	}
	fmt.Println("local time updated to",d.localTime)
	d.commitedDependencies = append(d.commitedDependencies, Dependency{
		Key: msg.Key, 
		Version: Version{ 
			Timestamp: msg.Timestamp ,
			DatacenterId: msg.DatacenterId,
		},
	})
	fmt.Println("dependencies commited:")
	for dp := range(d.commitedDependencies){
		fmt.Println(d.commitedDependencies[dp])
	}
	fmt.Println("Updated key",msg.Key,"to store",msg.Value)
}

func (d *Store) dependencyCheck(dependencies []Dependency) bool{
	var isFound = false
	for _,dependency:= range(dependencies){
		for _,commitedDep:= range(d.commitedDependencies){
			if commitedDep.Key == dependency.Key && versionCompare(commitedDep.Version,dependency.Version){
				isFound = true
			}
		}
		if !isFound{
			fmt.Println("Atleast one dependency not satisfied")
			return false
		} 
		isFound = false
	}
	fmt.Println("All dependencies satisfied")
	return true
}

func versionCompare(v1 Version, v2 Version) bool{
	return v1.DatacenterId==v2.DatacenterId && v1.Timestamp==v2.Timestamp
}
func main(){
	if len(os.Args)<2{
		fmt.Println("Missing datacenter number")
		return
	}
	val,_ := strconv.Atoi(os.Args[1])
	ports := getDatacenterPorts()
	found := false
	for port,id:= range(ports){
		if id==val{
			dataCenter.port = port
			dataCenter.id = id
			found=true
			break
		}
	}
	if !found{
		fmt.Println("Incorrect datacenter number")
		return
	}
	dataCenter.clients = make(map[string]*Client)
	dataCenter.data = make(map[string]string)
	var address = fmt.Sprintf("%s:%d", connIP, dataCenter.port)
	fmt.Println("Server running on port",dataCenter.port)//,address)
	listener, err := net.Listen("tcp", address)
	if err!=nil{
		log.Fatal(err)
	}
	for { // client/server connections
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error connecting:", err.Error())
		}
		go dataCenter.handleConnection(conn)
	}
}