package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/deis/deis/logger/syslogd"
)

var (
	logAddr         string
	logPort         int
	drainURI        string
	enablePublish   bool
	publishHost     string
	publishPath     string
	publishPort     string
	publishInterval int
	publishTTL      int
)

func init() {
	flag.StringVar(&logAddr, "log-addr", "0.0.0.0", "bind address for the logger")
	flag.IntVar(&logPort, "log-port", 514, "bind port for the logger")
	flag.StringVar(&drainURI, "drain-uri", "", "default drainURI, once set in etcd, this has no effect.")
	flag.StringVar(&syslogd.LogRoot, "log-root", "/data/logs", "log path to store logs")
	flag.BoolVar(&enablePublish, "enable-publish", false, "enable publishing to service discovery")
	flag.StringVar(&publishHost, "publish-host", getopt("HOST", "127.0.0.1"), "service discovery hostname")
	flag.IntVar(&publishInterval, "publish-interval", 10, "publish interval in seconds")
	flag.StringVar(&publishPath, "publish-path", getopt("ETCD_PATH", "/deis/logs"), "path to publish host/port information")
	flag.StringVar(&publishPort, "publish-port", getopt("ETCD_PORT", "4001"), "service discovery port")
	flag.IntVar(&publishTTL, "publish-ttl", publishInterval*2, "publish TTL in seconds")
}

func main() {
	flag.Parse()

	client := etcd.NewClient([]string{"http://" + publishHost + ":" + publishPort})

	signalChan := make(chan os.Signal, 1)
	drainRespChan := make(chan *etcd.Response)
	drainChan := make(chan string)
	stopChan := make(chan bool)
	exitChan := make(chan bool)
	cleanupChan := make(chan bool)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	// ensure the drain key exists in etcd.
	if _, err := client.Get(publishPath+"/drain", false, false); err != nil {
		setEtcd(client, publishPath+"/drain", drainURI, 0)
	}

	go client.Watch(publishPath+"/drain", 0, false, drainRespChan, stopChan)
	go syslogd.Listen(exitChan, cleanupChan, drainChan, fmt.Sprintf("%s:%d", logAddr, logPort))
	if enablePublish {
		go publishService(exitChan, client, publishHost, publishPath, strconv.Itoa(logPort), uint64(time.Duration(publishTTL).Seconds()))
	}

	for {
		select {
		case dr := <-drainRespChan:
			if dr != nil && dr.Node != nil {
				drainChan <- dr.Node.Value
			}
		case <-signalChan:
			close(exitChan)
			stopChan <- true
		case <-cleanupChan:
			return
		}
	}
}

func publishService(exitChan chan bool, client *etcd.Client, host string, etcdPath string, port string, ttl uint64) {
	t := time.NewTicker(time.Duration(publishInterval))

	for {
		select {
		case <-t.C:
			setEtcd(client, etcdPath+"/host", host, ttl)
			setEtcd(client, etcdPath+"/port", port, ttl)
		case <-exitChan:
			return
		}
	}
}

func setEtcd(client *etcd.Client, key, value string, ttl uint64) {
	_, err := client.Set(key, value, ttl)
	if err != nil && !strings.Contains(err.Error(), "Key already exists") {
		log.Println(err)
	}
}

func getopt(name, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
}
