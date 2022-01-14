package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

const (
	layout_time = "2006/02/01 15:04:05"
	nom_coa_os  = "OS_Q"
	nom_coa_pot = "POT_Q"
	URL         = "amqp://pep:pep@192.168.1.45:5672/"
)

var os_q amqp.Queue
var pot_q amqp.Queue

var connexio_servidor *amqp.Connection
var canal *amqp.Channel

func main() {
	StartConnexio()
	abella_msg, _ := canal.Consume(os_q.Name, "os", true, false, false, false, nil)
	fmt.Println(time.Now().Format(layout_time) + " [*] L'os dorm si no li donen menjar")
	BuidaPot()
	for i := 1; i <= 3; i++ {
		msg := <-abella_msg
		nom_abella := string(msg.Body[:])
		fmt.Println(time.Now().Format(layout_time) + " L'ha despertat l'abella " + nom_abella + " i menja " + strconv.Itoa(i) + "/3")
		for i := 0; i <= 3; i++ {
			time.Sleep(time.Second)
			fmt.Print(".")
		}
		fmt.Println()
		if i != 3 {
			fmt.Println(time.Now().Format(layout_time) + " L'os s'en va a dormir")
			BuidaPot()
		} else {
			fmt.Println("L'os romp el pot i ja no es pot produir més mel!")
			RomprePot()
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

func BuidaPot() {
	msg := 0
	publ := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(strconv.Itoa(msg)),
	}
	canal.Publish(
		"",
		pot_q.Name,
		false,
		false,
		publ)
}

func RomprePot() {
	msg := -1
	publ := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(strconv.Itoa(msg)),
	}
	canal.Publish(
		"",
		pot_q.Name,
		false,
		false,
		publ)
}
