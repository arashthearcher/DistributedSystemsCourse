// assignment3
package main

import (
	"./govec"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const nodeTimeOut = 2
const leaderHeartBeatKey = "leaderHeartBeat"
const leaderKey = "leader"
const heartBeatInterval = 1
const leaderTimeOut = 3
const unavail string = "unavailable"
const registeringPeriod = 2
const aliveListUpdateInterval = 3
const aliveNodesKey = "aliveNodes"

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

var logger *govec.GoLog
var lAddr *net.UDPAddr
var id string
var ipPort string
var client *rpc.Client
var registeredIDKey string
var heartBeat int64
var wg sync.WaitGroup
var keyCounter map[string]int64 = make(map[string]int64)
var mutex sync.Mutex
var idInt int64
var isLeader bool = false
var heartBeatKeys []string
var aliveIDs = make(map[string]bool)
var mapMutex sync.Mutex

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

func TestSet(Key, TestVal, Val string) string {
	testSetArgs := new(TestSetArgs)
	testSetArgs.Key = Key
	testSetArgs.NewVal = Val
	testSetArgs.TestVal = TestVal
	testSetArgs.VStamp = logSend("RPCing TestSet( " + Key + " , " + TestVal + " , " + Val + " )")

	reply := new(ValReply)

	client.Call("KeyValService.TestSet", &testSetArgs, &reply)
	logRecieve(ipPort+" "+"response TestSet( "+Key+" , "+TestVal+" , "+Val+" ) = "+reply.Val, reply.VStamp)
	return reply.Val

}

func Put(Key, Val string) string {
	putArgs := new(PutArgs)
	putArgs.Key = Key
	putArgs.Val = Val

	putArgs.VStamp = logger.PrepareSend(ipPort+" "+"RPCing Put( "+Key+" , "+Val+" )", nil)

	reply := new(ValReply)

	client.Call("KeyValService.Put", &putArgs, &reply)

	logRecieve(ipPort+" "+"response Put( "+Key+" , "+Val+" ) = "+reply.Val, reply.VStamp)

	return reply.Val

}

func Get(Key string) string {
	getArgs := new(GetArgs)
	getArgs.Key = Key
	getArgs.VStamp = logger.PrepareSend("RPCing Get( "+Key+" )", nil)

	reply := new(ValReply)

	client.Call("KeyValService.Get", &getArgs, &reply)

	logRecieve(ipPort+" "+"response Get( "+Key+" ) = "+reply.Val, reply.VStamp)

	return reply.Val

}

func setHeartBeat() {

	defer wg.Done()
	for {

		putNext(id, strconv.FormatInt(heartBeat, 10))
		heartBeat++
		time.Sleep(heartBeatInterval * time.Second)

	}

}

func setLeaderHeartBeat() {

	var leaderHeartBeat int64 = 0
	for {

		putNext(leaderHeartBeatKey, strconv.FormatInt(leaderHeartBeat, 10))
		leaderHeartBeat++
		time.Sleep(heartBeatInterval * time.Second)

	}

}

func registerID() string {

	var num int64 = 1
	response := ""
	for response != id || response == unavail {
		response = TestSet("r"+strconv.FormatInt(num, 10), "", id)
		num++
	}

	return strconv.FormatInt(num, 10)

}

func checkRegisteredIDKey(registeredIDKey string) {

	for {
		if Get(registeredIDKey) != id {
			registeredIDKey = registerID()
		}
		time.Sleep(registeringPeriod * time.Second)
	}

}

func getNext(key string) string {

	var response string = unavail

	for response == unavail {
		nextKey := key + "." + strconv.FormatInt(keyCounter[key], 10)
		response = Get(nextKey)
		if response == unavail {
			keyCounter[key]++
		}
	}

	return response

}

func putNext(key, val string) string {

	var response string = unavail

	for response == unavail {
		nextKey := key + "." + strconv.FormatInt(keyCounter[key], 10)
		response = Put(nextKey, val)
		if response == unavail {
			keyCounter[key]++
		}

	}

	return response

}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func readRegistredNodes() {
	var num int64 = 2
	response := unavail

	for {
		num--
		response = Get("r" + strconv.FormatInt(num, 10))
		for response != "" || response == unavail {
			response = Get("r" + strconv.FormatInt(num, 10))
			if response != "" && response != unavail {
				if !contains(heartBeatKeys, response) {
					mapMutex.Lock()
					heartBeatKeys = append(heartBeatKeys, response)
					go checkNodeHeartBeat(response)
					mapMutex.Unlock()
				}

			}
			num++
		}

		time.Sleep(registeringPeriod * time.Second)
	}

}

func collectTheAliveNodes() {

	for {

		var aliveNodesList []string

		for k, v := range aliveIDs {
			if v {
				aliveNodesList = append(aliveNodesList, k)
			}
		}

		aliveNodesString := fmt.Sprintf("%v", aliveNodesList)

		putNext(aliveNodesKey, aliveNodesString)

		time.Sleep(aliveListUpdateInterval * time.Second)
	}

}

func getAliveNodes() {

	for {

		getNext(aliveNodesKey)

		time.Sleep(aliveListUpdateInterval * time.Second)
	}

}

func beTheLeader() {

	logLocal("************" + id + " is leader now **************")

	isLeader = true
	go setLeaderHeartBeat()
	go readRegistredNodes()
	time.Sleep(nodeTimeOut * time.Second)
	go collectTheAliveNodes()

}

func electLeader() {

	for {
		currentCandidate, err := strconv.ParseInt(getNext(leaderKey), 10, 64)
		if err != nil {
			currentCandidate = 0
			currentCandidate, err = strconv.ParseInt(testAndSetNext(leaderKey, "", id), 10, 64)
		}

		for currentCandidate < idInt {
			currentCandidate, err = strconv.ParseInt(testAndSetNext(leaderKey, strconv.FormatInt(currentCandidate, 10), id), 10, 64)
			if err != nil {
				currentCandidate = 0
			}
			time.Sleep((leaderTimeOut / 2) * time.Second)
		}
		time.Sleep(leaderTimeOut * time.Second)
		currentCandidate, err = strconv.ParseInt(getNext(leaderKey), 10, 64)
		if err != nil {
			currentCandidate = 0
		}

		if currentCandidate == idInt {

			currentCandidate, _ = strconv.ParseInt(testAndSetNext(leaderKey, id, strconv.FormatInt(math.MaxInt64, 10)), 10, 64)
			if currentCandidate == math.MaxInt64 {
				beTheLeader()
				break
			}

		} else if currentCandidate > idInt {
			break
		}

		time.Sleep((leaderTimeOut / 2) * time.Second)
	}

}

func checkLeaderHeartBeat() {

	var leaderHeartBeat int64
	var counter int = 0
	for isLeader == false {

		newLeaderHeartBeat, _ := strconv.ParseInt(getNext(leaderHeartBeatKey), 10, 64)
		if newLeaderHeartBeat == leaderHeartBeat {
			counter++
		} else {
			leaderHeartBeat = newLeaderHeartBeat
			counter = 0
		}

		if counter > leaderTimeOut {

			logLocal("***********leader is down, selecting new leader*************")
			Put(leaderKey+"."+strconv.FormatInt(keyCounter[leaderKey], 10), unavail)
			electLeader()
			time.Sleep(leaderTimeOut * time.Second)
			counter = 0
		}

		time.Sleep(heartBeatInterval * time.Second)

	}
}

func checkNodeHeartBeat(nodeID string) {

	var nodeHeartBeat int64
	var counter int = 0
	for {

		response := getNext(nodeID)
		if response == unavail {
			return
		}

		newNodeHeartBeat, _ := strconv.ParseInt(response, 10, 64)
		if response == "" {
			newNodeHeartBeat = 0
		}
		if newNodeHeartBeat == nodeHeartBeat {
			counter++
		} else {
			if aliveIDs[nodeID] != true {
				mapMutex.Lock()
				aliveIDs[nodeID] = true
				mapMutex.Unlock()
			}

			nodeHeartBeat = newNodeHeartBeat
			counter = 0
		}

		if counter > nodeTimeOut {
			if aliveIDs[nodeID] != false {
				mapMutex.Lock()
				aliveIDs[nodeID] = false
				mapMutex.Unlock()
			}

			counter = 0
		}

		time.Sleep(heartBeatInterval * time.Second)

	}
}

func testAndSetNext(key, testVal, newVal string) string {

	var response string = unavail

	for response == unavail {
		nextKey := key + "." + strconv.FormatInt(keyCounter[key], 10)
		response = TestSet(nextKey, testVal, newVal)
		if response == unavail {
			keyCounter[key]++
		}
	}

	return response

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

func main() {

	ipPort = os.Args[1]

	addr, errL := net.ResolveUDPAddr("udp4", ipPort)
	logFatal(errL)
	lAddr = addr

	id = os.Args[2]

	var err error
	idInt, err = strconv.ParseInt(id, 10, 64)

	logFile := os.Args[3]

	// Initialize logger
	logger = govec.Initialize(id, logFile)
	logger.LogLocalEvent(id + " started...")

	rpcclient, err := rpc.Dial("tcp", ipPort)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	client = rpcclient

	registeredIDKey = registerID()

	wg.Add(1)
	go setHeartBeat()

	go checkLeaderHeartBeat()
	go getAliveNodes()

	wg.Wait()
}
