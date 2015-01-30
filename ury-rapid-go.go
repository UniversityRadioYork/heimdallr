package main

import "os"
import "fmt"
import "net"
import "bufio"
import "bytes"

import "github.com/UniversityRadioYork/ury-rapid-go/tokeniser"

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:1350")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	t := tokeniser.NewTokeniser()
	for {
		data, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		lines := t.Parse(data)
		buffer := new(bytes.Buffer)
		for _, line := range lines {
			for _, word := range line {
				buffer.WriteString(word + " ")
			}
			fmt.Println(buffer.String())
		}
	}
}
