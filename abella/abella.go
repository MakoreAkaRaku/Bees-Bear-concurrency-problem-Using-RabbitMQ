package main

// Autor: Marc Roman Colom
// link youtube: https://youtu.be/H_gPbAeVUEA
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
	URL         = "amqp://guest:guest@localhost:5672/"
)

var connexio_servidor *amqp.Connection
var canal *amqp.Channel

var os_q amqp.Queue
var sortida_q amqp.Queue
var pot_q amqp.Queue
var missatge_pot <-chan amqp.Delivery
var missatge_pot_romput <-chan amqp.Delivery
var nom_abella string

func main() {
	nom_abella = os.Args[len(os.Args)-1]
	StartConnexio()
	missatge_pot, _ = canal.Consume(pot_q.Name, nom_abella, true, false, false, false, nil)
	missatge_pot_romput, _ = canal.Consume(sortida_q.Name, "", true, true, false, false, nil)
	fmt.Println(time.Now().Format(layout_time) + " Soc l'abella " + nom_abella)
	//Quan surti de la funció principal, executarà el tancament del canal i de la connexió amb el servidor.
	defer canal.Close()
	defer connexio_servidor.Close()
	for {
		select {
		//L'abella demana permís per accedir al pot.
		case msg := <-missatge_pot:
			str_msg := string(msg.Body[:])
			nMel, _ := strconv.Atoi(str_msg)
			nMel++
			fmt.Println(time.Now().Format(layout_time) + " L'abella " + nom_abella + " produeix mel " + strconv.Itoa(nMel))
			if nMel == tamany_pot {
				//Notifica a l'ós de que el pot està ple.
				ReleaseMsgOs()
			} else {
				//Fica la unitat incrementada en un a la coa.
				ReleaseMsgPot(nMel)
			}
			// Cada punt es un segon de espera de l'abella
			for i := 0; i < 3; i++ {
				fmt.Print(".")
				time.Sleep(time.Second)
			}
			fmt.Println()
		// En cas de que arribi el missatge de que s'ha romput el pot, finalitza.
		case <-missatge_pot_romput:
			fmt.Println(time.Now().Format(layout_time) + " " + nom_abella + ": L'os ha romput el pot, a reveure!")
			return
		}

	}
}

//Inicialitza la connexió amb el servidor i declara les coes per a la simulació
func StartConnexio() {
	connexio_servidor, _ = amqp.Dial(URL)
	canal, _ = connexio_servidor.Channel()
	os_q, _ = canal.QueueDeclare(nom_coa_os, true, true, false, false, nil)
	pot_q, _ = canal.QueueDeclare(nom_coa_pot, true, true, false, false, nil)
	sortida_q, _ = canal.QueueDeclare("", true, true, false, false, nil)
	canal.ExchangeDeclare("amq.fanout", "fanout", true, false, false, false, nil)
	canal.QueueBind(sortida_q.Name, nom_abella, "amq.fanout", false, nil)
}

//Envia un missatge per notificar a l'os de que el pot está ple.
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

//Envia un missatge a la coa del pot de mel amb les unitats de mel actualitzades.
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
