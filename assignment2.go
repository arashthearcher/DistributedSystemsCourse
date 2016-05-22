package main

import (
	"./govec"
	"bufio"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var logger *govec.GoLog
var localTime Time
var lAddr *net.UDPAddr
var deltas []int64
var timeouts []bool
var received []bool
var wg sync.WaitGroup
var ipPort string

const T = 5
const timeout = 250

type Time struct {
	localTime  int64
	systemTime int64
}

func (t *Time) setTime(now int64) {
	t.localTime = now
	t.systemTime = (time.Now().UnixNano() * int64(time.Nanosecond)) / int64(time.Millisecond)
}

func (t *Time) changeTimeBy(offset int64) {
	//	t.setTime(t.getTime() + offset)
	currentSystemTime := (time.Now().UnixNano() * int64(time.Nanosecond)) / int64(time.Millisecond)
	millisPast := currentSystemTime - t.systemTime
	t.localTime = t.localTime + millisPast + offset
	t.systemTime = currentSystemTime
}

func (t *Time) getTime() int64 {

	currentSystemTime := (time.Now().UnixNano() * int64(time.Nanosecond)) / int64(time.Millisecond)
	millisPast := currentSystemTime - t.systemTime
	return t.localTime + millisPast
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

func initializeArrays(n int) {

	deltas = make([]int64, n)
	timeouts = make([]bool, n)
	received = make([]bool, n)

}

func falseOutArrays(n int) {
	for i := 0; i < n; i++ {
		timeouts[i] = false
		received[i] = false
	}
}

func readSlaveIPs(fileName string) []*net.UDPAddr {

	var slavesAddrs []*net.UDPAddr

	file, err := os.Open(fileName)

	logFatal(err)

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		addrString := scanner.Text()
		addr, err := net.ResolveUDPAddr("udp4", addrString)
		logFatal(err)
		slavesAddrs = append(slavesAddrs, addr)
	}

	return slavesAddrs
}

func requestTimeFromSlaveAndComputeDelta(rAddr *net.UDPAddr, num, roundNumber int) {

	defer wg.Done()

	conn, errDial := net.DialUDP("udp", nil, rAddr)
	printErr(errDial)

	defer conn.Close()

	// sending UDP packet to specified address and port

	errDeadLine := conn.SetDeadline(time.Now().Add(time.Duration(timeout * time.Millisecond)))
	if errDeadLine != nil {
		log.Println(errDeadLine.Error())
		timeouts[num] = true
		return
	}

	msg := []byte(strconv.Itoa(roundNumber) + ",ASK,")
	logedMSG := logger.PrepareSend(ipPort+" "+strconv.FormatInt(localTime.getTime(), 10)+" asking slave "+strconv.Itoa(num)+" for its time", msg)

	t1 := localTime.getTime()

	_, errWrite := conn.Write(logedMSG)
	if errWrite != nil {
		log.Println(errWrite.Error())
		timeouts[num] = true
		return
	}

	logger.LogLocalEvent(ipPort + " " + strconv.FormatInt(localTime.getTime(), 10) + " message sent to slave " + strconv.Itoa(num) + " ,waiting for response ")

	for {

		// Reading the response message
		var buf [1500]byte
		n, errRead := conn.Read(buf[0:])
		if errRead != nil {
			log.Println(errRead.Error())
			timeouts[num] = true
			return
		}

		t3 := localTime.getTime()

		message := logger.UnpackReceive(ipPort+" "+strconv.FormatInt(localTime.getTime(), 10)+" Received Time From Slave "+strconv.Itoa(num), buf[:n])

		slicedMessage := strings.Split(string(message), ",")

		rndNumber, parseErr := strconv.Atoi(slicedMessage[0])
		printErr(parseErr)
		timeString := slicedMessage[1]

		if rndNumber == roundNumber {
			t2, errAtoi := strconv.ParseInt(string(timeString), 10, 64)
			printErr(errAtoi)

			deltas[num] = t2 - (t1+t3)/2
			received[num] = true
			return
		}

	}

}

type Int64s []int64

func (a Int64s) Len() int           { return len(a) }
func (a Int64s) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Int64s) Less(i, j int) bool { return a[i] < a[j] }

// this fault tolerant averaging function tries to minimize number of faulty processes while maintaing the threshold property
// run time of algorithm at worst case is n^2
// I start with two pointers i and j for begining and end of sorted array and do a loop on i + j
func computeFaultTolerantAVG(numbers []int64, d int64) int64 {
	sort.Sort(Int64s(numbers))

	var i, j int
OuterLoop:
	for k := 0; k < len(numbers)-1; k++ {
		for i = 0; i <= k; i++ {
			j = k - i
			if numbers[len(numbers)-j-1]-numbers[i] < d {
				break OuterLoop
			}
		}
	}

	return average(numbers[i:(len(numbers) - j)])

}

// why golang does not have a built-in average func ?! :D
func average(numbers []int64) int64 {

	var sum int64 = 0
	for i := 0; i < len(numbers); i++ {
		sum += numbers[i]
	}
	return sum / int64(len(numbers))
}

func getArrayOfReceivedTimes() []int64 {
	var arr []int64
	// appending master diff
	arr = append(arr, int64(0))
	for i := 0; i < len(deltas); i++ {
		if received[i] && !timeouts[i] {
			arr = append(arr, deltas[i])
		}
	}

	return arr
}

func sendDeltas(rAddr *net.UDPAddr, avg int64, num, roundNumber int) {
	defer wg.Done()

	conn, errDial := net.DialUDP("udp", nil, rAddr)
	printErr(errDial)

	defer conn.Close()

	// sending UDP packet to specified address and port

	errDeadLine := conn.SetDeadline(time.Now().Add(time.Duration(timeout * time.Millisecond)))
	if errDeadLine != nil {
		log.Println(errDeadLine.Error())
		return
	}

	msg := []byte(strconv.Itoa(roundNumber) + ",SET," + strconv.FormatInt(avg-deltas[num], 10))
	logedMSG := logger.PrepareSend(ipPort+" "+strconv.FormatInt(localTime.getTime(), 10)+" asking slave "+strconv.Itoa(num)+" to set its time", msg)

	_, errWrite := conn.Write(logedMSG)
	if errWrite != nil {
		log.Println(errWrite.Error())
		timeouts[num] = true
	}

}

func main() {

	if os.Args[1] == "-m" {
		// input example : go run assignment2.go -m localhost:8080 1000 50000 slaves.txt master
		// fetch arguments for ease of use
		ipPort = os.Args[2]

		addr, errL := net.ResolveUDPAddr("udp4", ipPort)
		logFatal(errL)
		lAddr = addr

		t, errTime := strconv.ParseInt(os.Args[3], 10, 64)
		logFatal(errTime)

		localTime.setTime(int64(t))

		d, errD := strconv.ParseInt(os.Args[4], 10, 64)
		logFatal(errD)

		slavesFile := os.Args[5]
		logFile := os.Args[6]

		// Initialize logger
		logger = govec.Initialize("master", logFile)
		logger.LogLocalEvent("Master started...")

		slavesAddrs := readSlaveIPs(slavesFile)

		log.Println("number of slaves ", len(slavesAddrs))

		initializeArrays(len(slavesAddrs))

		roundNumber := 1

		for {
			falseOutArrays(len(slavesAddrs))
			for i := 0; i < len(slavesAddrs); i++ {
				wg.Add(1)
				go requestTimeFromSlaveAndComputeDelta(slavesAddrs[i], i, roundNumber)
			}

			wg.Wait()

			avg := computeFaultTolerantAVG(getArrayOfReceivedTimes(), d)

			logger.LogLocalEvent(ipPort + " " + strconv.FormatInt(localTime.getTime(), 10) + " computed avg : " + strconv.FormatInt(avg, 10))

			for i := 0; i < len(slavesAddrs); i++ {
				if received[i] && !timeouts[i] {
					wg.Add(1)
					go sendDeltas(slavesAddrs[i], avg, i, roundNumber)
				}

			}

			wg.Wait()

			logger.LogLocalEvent(ipPort + " " + strconv.FormatInt(localTime.getTime(), 10) + " changing local time by " + strconv.FormatInt(avg, 10))
			localTime.changeTimeBy(avg)
			logger.LogLocalEvent(ipPort + " " + strconv.FormatInt(localTime.getTime(), 10) + " local time changed by " + strconv.FormatInt(avg, 10))
			roundNumber++
			time.Sleep(T * time.Second)
		}

	} else if os.Args[1] == "-s" {
		//		go run assignment2.go -s :8081 1000 slave1
		//				go run assignment2.go -s :8082 1000 slave2
		ipPort = os.Args[2]
		addr, errL := net.ResolveUDPAddr("udp4", ipPort)
		logFatal(errL)
		lAddr = addr

		t, errTime := strconv.ParseInt(os.Args[3], 10, 64)
		logFatal(errTime)

		localTime.setTime(int64(t))

		logFile := os.Args[4]

		logger = govec.Initialize(logFile, logFile)
		logger.LogLocalEvent(ipPort + " started...")

		conn, err := net.ListenPacket("udp", ipPort)
		printErr(err)

		var roundNumber int = -1

		for {
			var buf [1500]byte

			n, addr, err := conn.ReadFrom(buf[0:])
			if err != nil {
				log.Println(err.Error())
				continue
			}
			incommingMSG := logger.UnpackReceive(ipPort+" "+strconv.FormatInt(localTime.getTime(), 10)+" Received Request from master", buf[:n])

			message := strings.Split(string(incommingMSG), ",")

			rndNumber, parseErr := strconv.Atoi(message[0])
			printErr(parseErr)
			if rndNumber < roundNumber {
				continue
			}

			if string(message[1]) == "ASK" {

				roundNumber = rndNumber
				msg := strconv.Itoa(roundNumber) + "," + strconv.FormatInt(localTime.getTime(), 10)
				logedMSG := logger.PrepareSend(ipPort+" "+strconv.FormatInt(localTime.getTime(), 10)+" sending server the time", []byte(msg))
				_, errW := conn.WriteTo(logedMSG, addr)
				if errW != nil {
					log.Println(err.Error())
					continue
				}
			} else if string(message[1]) == "SET" && rndNumber == roundNumber {
				delta, errParse := strconv.ParseInt(string(message[2]), 10, 64)
				printErr(errParse)
				logger.LogLocalEvent(ipPort + " " + strconv.FormatInt(localTime.getTime(), 10) + " changing local time by " + strconv.FormatInt(delta, 10))
				localTime.changeTimeBy(delta)
				logger.LogLocalEvent(ipPort + " " + strconv.FormatInt(localTime.getTime(), 10) + " local time changed by " + strconv.FormatInt(delta, 10))

			} else {
				logger.LogLocalEvent(ipPort + " " + strconv.FormatInt(localTime.getTime(), 10) + " received old message from master with round number " + strconv.Itoa(rndNumber))
			}

		}

		conn.Close()
	}

}
