package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

const (
	tamany_pot  = 10
	layout_time = "2006/02/01 15:04:05"
	nom_coa_pot = "POT_Q"
	nom_coa_os  = "OS_Q"
	URL         = "amqp://pep:pep@192.168.1.45:5672/"
)

var connexio_servidor *amqp.Connection
var canal *amqp.Channel

var os_q amqp.Queue
var pot_q amqp.Queue
var missatge_pot <-chan amqp.Delivery
var nom_abella string

func main() {
	StartConnexio()
	nom_abella = os.Args[len(os.Args)-1]
	missatge_pot, _ = canal.Consume(pot_q.Name, nom_abella, true, false, false, false, nil)
	fmt.Println(time.Now().Format(layout_time) + " Soc l'abella " + nom_abella)
	for {
		nMel := AcquireMsgPot()
		if nMel >= 0 {
			nMel++
			fmt.Println(time.Now().Format(layout_time) + " L'abella " + nom_abella + " produeix mel " + strconv.Itoa(nMel))
			if nMel == tamany_pot {
				ReleaseMsgOs()
			} else {
				ReleaseMsgPot(nMel)
			}

			for i := 0; i < 3; i++ {
				fmt.Print(".")
				time.Sleep(time.Second)
			}
			fmt.Println()
		} else {
			fmt.Println(time.Now().Format(layout_time) + " " + nom_abella + ": L'os ha romput el pot, a reveure!")
			ReleaseMsgPot(nMel)
			break
		}

	}
	defer canal.Close()
	defer connexio_servidor.Close()
}

func StartConnexio() {
	connexio_servidor, _ = amqp.Dial(URL)
	canal, _ = connexio_servidor.Channel()
	os_q, _ = canal.QueueDeclare(nom_coa_os, true, true, false, false, nil)
	pot_q, _ = canal.QueueDeclare(nom_coa_pot, true, true, false, false, nil)
}

func AcquireMsgPot() int {
	msg := <-missatge_pot
	str_msg := string(msg.Body[:])
	nMel, _ := strconv.Atoi(str_msg)
	return nMel
}

func ReleaseMsgOs() {
	fmt.Println("L'abella " + nom_abella + " notifica a l'os")
	publ := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(nom_abella),
	}
	canal.Publish(
		"",
		os_q.Name,
		false,
		false,
		publ)
}
func ReleaseMsgPot(nMel int) {
	str_msg := strconv.Itoa(nMel)

	publ := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(str_msg),
	}

	canal.Publish(
		"",
		pot_q.Name,
		false,
		false,
		publ)
}
