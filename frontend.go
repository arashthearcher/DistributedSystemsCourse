package main

import (
	"./govec"
	"bytes"
	"container/heap"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
	//"strconv"
)

// Global Vars

// Reserved value in the service that is used to indicate that the key
// is unavailable: used in return values to clients and internally.
const unavail string = "unavailable"
const ReBalancePeriod = 5

var logger *govec.GoLog

var frontEndIpPort string
var clientsIpPort string

var rpcClients map[string]*rpc.Client
var mutex sync.Mutex
var heapMutex sync.Mutex
var mapMutex sync.RWMutex
var loadHeap *kvnodeHeap
var assignedKVNodeToKey map[string]string
var assignedKeysToKVNode map[string][]string

type KeyValService int
type RegistrationService int

// An IntHeap is a min-heap of ints.
type kvnodeHeap []kvnodeLoad

type kvnodeLoad struct {
	IpPort string
	Load   int
}

func (h kvnodeHeap) Len() int           { return len(h) }
func (h kvnodeHeap) Less(i, j int) bool { return h[i].Load < h[j].Load }
func (h kvnodeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h kvnodeHeap) String() string {
	var buffer bytes.Buffer

	for i := 0; i < len(h); i++ {
		buffer.WriteString(h[i].IpPort + " : " + strconv.Itoa(h[i].Load) + "    ")
	}

	return buffer.String()
}

func (h *kvnodeHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(kvnodeLoad))
}

func (h *kvnodeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func initializeHeap() {
	loadHeap = new(kvnodeHeap)
	heap.Init(loadHeap)
}

func getMostUnLoadedServerAndIncrementItBy(amount int) string {

	if len(*loadHeap) > 0 {
		heapMutex.Lock()

		ipPort := (*loadHeap)[0].IpPort
		(*loadHeap)[0].Load += amount
		heap.Fix(loadHeap, 0)

		heapMutex.Unlock()

		return ipPort
	}

	return unavail

}

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

func TestSetRPC(Key, TestVal, Val, KVNode string) (string, error) {
	testSetArgs := new(TestSetArgs)
	testSetArgs.Key = Key
	testSetArgs.NewVal = Val
	testSetArgs.TestVal = TestVal
	testSetArgs.VStamp = logSend("RPCing TestSet( " + Key + " , " + TestVal + " , " + Val + " )")

	reply := new(ValReply)
	client := rpcClients[KVNode]
	err := client.Call("KeyValService.TestSet", &testSetArgs, &reply)

	if err != nil {
		deleteKVNode(KVNode)
		return unavail, *new(error)
	} else {
		logRecieve(frontEndIpPort+" "+"response TestSet( "+Key+" , "+TestVal+" , "+Val+" ) = "+reply.Val, reply.VStamp)
	}
	return reply.Val, nil

}

func PutRPC(Key, Val, KVNode string) (string, error) {
	putArgs := new(PutArgs)
	putArgs.Key = Key
	putArgs.Val = Val

	putArgs.VStamp = logSend(frontEndIpPort + " " + "RPCing Put( " + Key + " , " + Val + " )")

	reply := new(ValReply)
	client := rpcClients[KVNode]
	err := client.Call("KeyValService.Put", &putArgs, &reply)

	if err != nil {
		deleteKVNode(KVNode)
		return unavail, *new(error)
	} else {
		logRecieve(frontEndIpPort+" "+"response Put( "+Key+" , "+Val+" ) = "+reply.Val, reply.VStamp)
	}
	return reply.Val, nil

}

func GetRPC(Key, KVNode string) (string, error) {
	getArgs := new(GetArgs)
	getArgs.Key = Key
	getArgs.VStamp = logSend("RPCing Get( " + Key + " )")

	reply := new(ValReply)

	client := rpcClients[KVNode]
	err := client.Call("KeyValService.Get", &getArgs, &reply)

	if err != nil {
		deleteKVNode(KVNode)
		return unavail, *new(error)
	} else {
		logRecieve(frontEndIpPort+" "+"response Get( "+Key+" ) = "+reply.Val, reply.VStamp)
	}

	return reply.Val, nil

}

func deleteKVNodeFromHeap(KVNode string) {
	for i := 0; i < len(*loadHeap); i++ {
		if (*loadHeap)[i].IpPort == KVNode {
			heap.Remove(loadHeap, i)
			return
		}
	}
}

func markAssociatedKeysUnavailable(KVNode string) {
	keys := assignedKeysToKVNode[KVNode]
	for i := 0; i < len(keys); i++ {
		assignedKVNodeToKey[keys[i]] = unavail
	}

	delete(assignedKeysToKVNode, KVNode)
}

func deleteKVNode(KVNode string) {
	mapMutex.Lock()
	defer mapMutex.Unlock()
	deleteKVNodeFromHeap(KVNode)
	markAssociatedKeysUnavailable(KVNode)
	delete(rpcClients, KVNode)
}

func computeAverage() int {

	sum := 0
	for i := 0; i < len(*loadHeap); i++ {
		sum = sum + (*loadHeap)[i].Load
	}
	avg := (sum / len(*loadHeap))
	return avg
}

func ReBalanceKVNode(index, avg int) bool {
	KVNode := (*loadHeap)[index].IpPort

	//fmt.Printf("**************** %d ******************** %d", len(assignedKeysToKVNode[KVNode]), (*loadHeap)[index].Load)

	keysToReBalance := assignedKeysToKVNode[KVNode][avg:]

	for i := 0; i < len(keysToReBalance); i++ {

		key := keysToReBalance[i]
		val, err := GetRPC(key, KVNode)
		if err != nil {
			return false
		}

		newKVNode := getMostUnLoadedServerAndIncrementItBy(1)
		if newKVNode != unavail {
			_, errPut := PutRPC(key, val, newKVNode)
			if errPut == nil {

				mapMutex.Lock()
				assignedKVNodeToKey[key] = newKVNode
				assignedKeysToKVNode[newKVNode] = append(assignedKeysToKVNode[newKVNode], key)
				mapMutex.Unlock()

				PutRPC(key, "", KVNode)
				logLocal(key + " moved from " + KVNode + " to " + newKVNode)
			} else {
				return false
			}
		} else {
			return false
		}

	}
	mapMutex.Lock()
	heapMutex.Lock()
	if _, ok := assignedKeysToKVNode[KVNode]; ok {
		(*loadHeap)[index].Load = avg
		heap.Fix(loadHeap, index)
		assignedKeysToKVNode[KVNode] = assignedKeysToKVNode[KVNode][:avg]
	}
	heapMutex.Unlock()
	mapMutex.Unlock()

	return true
}

func ReBalanceKeys() {

	for {
		time.Sleep(ReBalancePeriod * time.Second)
		if len(*loadHeap) > 1 {
			avg := computeAverage()
			for i := len(*loadHeap) - 1; i >= 0; i-- {
				if (*loadHeap)[i].Load > avg {
					ret := ReBalanceKVNode(i, avg)
					if ret == false {
						break
					}
				}
			}
			logLocal("rebalanced ! avg is " + strconv.Itoa(avg) + " loads right now :" + (*loadHeap).String())
		}

	}

}

func (rs *RegistrationService) Register(args *RegArgs, reply *ValReply) error {

	logRecieve("Register("+args.IpPort+")", args.VStamp)
	if _, ok := assignedKeysToKVNode[args.IpPort]; ok {
		deleteKVNode(args.IpPort)
	}
	heapMutex.Lock()
	newkvnodeLoad := kvnodeLoad{args.IpPort, 0}
	heap.Push(loadHeap, newkvnodeLoad)
	heapMutex.Unlock()

	// create the RPC client to connect to this kvnode
	var err error
	rpcClients[args.IpPort], err = rpc.Dial("tcp", args.IpPort)
	printErr(err)

	reply.Val = args.IpPort + " Registred!"
	reply.VStamp = logSend("Register-re:" + reply.Val)
	return nil

}

// Lookup a key, and if it's used for the first time, then initialize its value.
func lookupKey(key string) string {

	if val, ok := assignedKVNodeToKey[key]; ok {
		return val
	} else {
		KVNode := getMostUnLoadedServerAndIncrementItBy(1)
		assignedKVNodeToKey[key] = KVNode
		if KVNode != unavail {
			assignedKeysToKVNode[KVNode] = append(assignedKeysToKVNode[KVNode], key)
		}

		return KVNode
	}

}

// GET
func (kvs *KeyValService) Get(args *GetArgs, reply *ValReply) error {

	logRecieve("get(k:"+args.Key+")", args.VStamp)

	mapMutex.RLock()
	KVNode := lookupKey(args.Key)
	mapMutex.RUnlock()

	if KVNode != unavail {
		reply.Val, _ = GetRPC(args.Key, KVNode)

	} else {
		reply.Val = unavail
	}

	reply.VStamp = logSend("get-re: " + reply.Val)

	return nil
}

// PUT
func (kvs *KeyValService) Put(args *PutArgs, reply *ValReply) error {

	logRecieve("put(k:"+args.Key+",v:"+args.Val+")", args.VStamp)

	mapMutex.RLock()
	KVNode := lookupKey(args.Key)
	mapMutex.RUnlock()

	if KVNode != unavail {
		reply.Val, _ = PutRPC(args.Key, args.Val, KVNode)

	} else {
		reply.Val = unavail
	}

	reply.VStamp = logSend("put-re: " + reply.Val)

	return nil
}

// TESTSET
func (kvs *KeyValService) TestSet(args *TestSetArgs, reply *ValReply) error {

	logRecieve("testset(k:"+args.Key+",tv:"+args.TestVal+",nv:"+args.NewVal+")", args.VStamp)

	mapMutex.RLock()
	KVNode := lookupKey(args.Key)
	mapMutex.RUnlock()

	if KVNode != unavail {
		reply.Val, _ = TestSetRPC(args.Key, args.TestVal, args.NewVal, KVNode)

	} else {
		reply.Val = unavail
	}

	reply.VStamp = logSend("testset-re: " + reply.Val)

	return nil
}

// Main server loop.
func main() {

	Init()
	go SetupRegistrationServer()
	go ReBalanceKeys()
	SetupClientRPCServer()
}

func SetupRegistrationServer() {
	rservice := new(RegistrationService)
	rpc.Register(rservice)
	l, e := net.Listen("tcp", frontEndIpPort)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
}

func Init() {
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port ip:port logfile\n", os.Args[0])
	if len(os.Args) != 4 {
		fmt.Println(len(os.Args))
		fmt.Printf(usage)
		os.Exit(1)
	}

	clientsIpPort = os.Args[1]
	frontEndIpPort = os.Args[2]
	logFile := os.Args[3]

	// Initialize logger
	logger = govec.Initialize(logFile, logFile)
	logger.LogLocalEvent(logFile + " started...")
	initializeHeap()

	rpcClients = make(map[string]*rpc.Client)
	assignedKVNodeToKey = make(map[string]string)
	assignedKeysToKVNode = make(map[string][]string)

}

func SetupClientRPCServer() {
	// setup key-value store and register service

	kvservice := new(KeyValService)
	rpc.Register(kvservice)
	l, e := net.Listen("tcp", clientsIpPort)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
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
