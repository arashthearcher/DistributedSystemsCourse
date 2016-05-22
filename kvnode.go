// Version 1.0
//
// A simple key-value store that supports three API calls over rpc:
// - get(key)
// - put(key,val)
// - testset(key,testval,newval)
//
// Usage: go run kvservicemainl.go ip:port key-fail-prob
// - ip:port : the ip and port on which the service will listen for connections
// - key-fail-prob : probability in range [0,1] of the key becoming
//   unavailable during one of the above operations (permanent key unavailability)
//
// TODOs:
// - needs some serious refactoring
// - simulate netw. partitioning failures
//
// Dependencies:
// - GoVector: https://github.com/arcaneiceman/GoVector

package main

import (
	"./govec"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	//"strconv"
)

// Global Vars

var logger *govec.GoLog
var frontEndIpPort string
var serviceIpPort string
var client *rpc.Client
var mutex sync.Mutex
var logFile string

type RegArgs struct {
	IpPort string
	VStamp []byte
}

// args in get(args)
type GetArgs struct {
	Key    string // key to look up
	VStamp []byte // vstamp(nil)
}

// args in put(args)
type PutArgs struct {
	Key    string // key to associate value with
	Val    string // value
	VStamp []byte // vstamp(nil)
}

// args in testset(args)
type TestSetArgs struct {
	Key     string // key to test
	TestVal string // value to test against actual value
	NewVal  string // value to use if testval equals to actual value
	VStamp  []byte // vstamp(nil)
}

// Reply from service for all three API calls above.
type ValReply struct {
	Val    string // value; depends on the call
	VStamp []byte // vstamp(nil)
}

// Value in the key-val store.
type MapVal struct {
	value  string       // the underlying value representation
	logger *govec.GoLog // GoVector instance for the *key* that this value is mapped to
}

// Map implementing the key-value store.
var kvmap map[string]*MapVal

// Reserved value in the service that is used to indicate that the key
// is unavailable: used in return values to clients and internally.
const unavail string = "unavailable"

type KeyValService int

// Lookup a key, and if it's used for the first time, then initialize its value.
func lookupKey(key string) *MapVal {
	// lookup key in store
	val := kvmap[key]
	if val == nil {
		// key used for the first time: create and initialize a MapVal instance to associate with a key
		val = &MapVal{
			value:  "",
			logger: govec.Initialize(logFile+"-key-"+key, logFile+"key-"+key),
		}
		kvmap[key] = val
	}
	return val
}

// GET
func (kvs *KeyValService) Get(args *GetArgs, reply *ValReply) error {
	val := lookupKey(args.Key)
	val.logger.UnpackReceive("get(k:"+args.Key+")", args.VStamp)

	reply.Val = val.value // execute the get
	reply.VStamp = val.logger.PrepareSend("get-re:"+val.value, nil)
	return nil
}

// PUT
func (kvs *KeyValService) Put(args *PutArgs, reply *ValReply) error {
	val := lookupKey(args.Key)
	val.logger.UnpackReceive("put(k:"+args.Key+",v:"+args.Val+")", args.VStamp)

	val.value = args.Val // execute the put
	reply.Val = ""
	reply.VStamp = val.logger.PrepareSend("put-re", nil)
	return nil
}

// TESTSET
func (kvs *KeyValService) TestSet(args *TestSetArgs, reply *ValReply) error {
	val := lookupKey(args.Key)
	val.logger.UnpackReceive("testset(k:"+args.Key+",tv:"+args.TestVal+",nv:"+args.NewVal+")", args.VStamp)

	// execute the testset
	if val.value == args.TestVal {
		val.value = args.NewVal
	}

	reply.Val = val.value
	reply.VStamp = val.logger.PrepareSend("testset-re:"+val.value, nil)
	return nil
}

// Main server loop.
func main() {
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	frontEndIpPort := os.Args[1]

	logFile = os.Args[2]

	// Initialize logger
	logger = govec.Initialize(logFile, logFile)
	logger.LogLocalEvent(logFile + " started...")

	rpcclient, err := rpc.Dial("tcp", frontEndIpPort)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	client = rpcclient

	// setup key-value store and register service
	kvmap = make(map[string]*MapVal)
	kvservice := new(KeyValService)
	rpc.Register(kvservice)
	l, e := net.Listen("tcp", "")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	serviceIpPort = l.Addr().String()

	Register()

	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}

}

func Register() {

	regArg := new(RegArgs)
	regArg.IpPort = serviceIpPort
	regArg.VStamp = logSend("RPCing Register( " + regArg.IpPort + " )")

	reply := new(ValReply)
	client.Call("RegistrationService.Register", &regArg, &reply)

	logRecieve(serviceIpPort+" "+"response Register( "+regArg.IpPort+" ) = "+reply.Val, reply.VStamp)
}

func logFatal(err error) {
	if err != nil {
		logger.LogLocalEvent(err.Error())
		os.Exit(1)
	}
}

func printErr(err error) {
	if err != nil {
		log.Println(err)
	}
}

func logSend(msg string) []byte {
	mutex.Lock()
	logged := logger.PrepareSend(msg, nil)
	mutex.Unlock()

	return logged

}

func logRecieve(msg string, buf []byte) {
	mutex.Lock()
	logger.UnpackReceive(msg, buf)
	mutex.Unlock()
}

func logLocal(msg string) {
	mutex.Lock()
	logger.LogLocalEvent(msg)
	mutex.Unlock()
}
