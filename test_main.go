package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func writeSome(conn net.Conn) {
	fmt.Println("start listening on scanner")
	_, _ = conn.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
}

func main() {
	address := "127.0.0.1:6379"
	conn, _ := net.Dial("tcp", address)

	go writeSome(conn)
	reader := bufio.NewReader(conn)
	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			log.Fatalln("error reading: ", err)
		}

		fmt.Printf("recieve: line: %+v\tisPrefix: %+v\n", string(line), isPrefix)
	}
}
