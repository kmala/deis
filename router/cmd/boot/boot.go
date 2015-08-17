package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/coreos/go-etcd/etcd"
	"github.com/deis/deis/router/logger"
)

var log = logrus.New()

const (
	timeout time.Duration = 10 * time.Second
	ttl     time.Duration = timeout * 2
)

func main() {
	log.Formatter = new(logger.StdOutFormatter)

	logLevel := getopt("LOG", "info")
	if level, err := logrus.ParseLevel(logLevel); err == nil {
		log.Level = level
	}

	log.Debug("reading environment variables...")
	host := getopt("HOST", "127.0.0.1")

	etcdPort := getopt("ETCD_PORT", "4001")

	etcdPath := getopt("ETCD_PATH", "/deis/router")

	//	hostEtcdPath := getopt("HOST_ETCD_PATH", "/deis/router/hosts/"+host)

	//externalPort := getopt("EXTERNAL_PORT", "80")

	client := etcd.NewClient([]string{"http://" + host + ":" + etcdPort})

	// wait until etcd has discarded potentially stale values
	time.Sleep(timeout + 1)

	log.Debug("creating required defaults in etcd...")
	mkdirEtcd(client, "/deis/controller")
	mkdirEtcd(client, "/deis/services")
	mkdirEtcd(client, "/deis/domains")
	mkdirEtcd(client, "/deis/builder")
	mkdirEtcd(client, "/deis/certs")
	//mkdirEtcd(client, "/deis/router/hosts")
	mkdirEtcd(client, "/deis/router/hsts")
	mkdirEtcd(client, "/registry/services/specs/default")

	setDefaultEtcd(client, etcdPath+"/gzip", "on")

	log.Info("Starting Nginx...")

	nginxChan := make(chan bool)
	go launchNginx(nginxChan)
	<-nginxChan

	// FIXME: have to launch cron first so generate-certs will generate the files nginx requires
	go launchCron()

	waitForInitialConfd(host+":"+etcdPort, timeout)

	go launchConfd(host + ":" + etcdPort)

	//go publishService(client, hostEtcdPath, host, externalPort, uint64(ttl.Seconds()))
	go publishApps(client, uint64(ttl.Seconds()))

	log.Info("deis-router running...")

	exitChan := make(chan os.Signal, 2)
	signal.Notify(exitChan, syscall.SIGTERM, syscall.SIGINT)
	<-exitChan
}

func publishApps(etcdClient *etcd.Client, ttl uint64) {
	/*client, err := client.NewInCluster()
	if err != nil {
		log.Fatalf("error getting client: %v", err)
	}
	namespaces, err := client.Namespaces().List(labels.Everything(), fields.Everything())
	if err != nil {
		log.Fatalf("error getting namespaces: %v", err)
	}
	for _, namespace := range namespaces.Items {
		log.Info("name is", namespace.ObjectMeta.Name)
	}*/
	log.Info("in publish apps ")
	tlsConfig := &tls.Config{}
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"); os.IsNotExist(err) {
		tlsConfig.InsecureSkipVerify = false
	} else {
		CaData, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
		if err != nil {
			log.Fatalf("can't read from the certificate: %v", err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(CaData)
		tlsConfig.RootCAs = certPool
		tlsConfig.MinVersion = tls.VersionTLS10
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	token, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		log.Fatalf("can't read the token: %v", err)
	}
	client := &http.Client{Transport: transport}

	for {
		host := getopt("KUBERNETES_SERVICE_HOST", "127.0.0.1")
		port := getopt("KUBERNETES_SERVICE_PORT", "443")
		servURL := "https://" + host + ":" + port + "/api/v1/namespaces/"
		servReq, err := http.NewRequest("GET", servURL, nil)
		servReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", string(token)))
		if err != nil {
			log.Fatalf("can't connect to the apiserver: %v", err)
		}
		//servClient := &http.Client{}
		servResp, err := client.Do(servReq)
		if err != nil {
			log.Fatalf("error in sending the request: %v", err)
		}
		body, _ := ioutil.ReadAll(servResp.Body)
		servResp.Body.Close()
		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		nameSpaces := data["items"].([]interface{})
		for i := range nameSpaces {
			nameSpace := nameSpaces[i].(map[string]interface{})
			metadata := nameSpace["metadata"].(map[string]interface{})
			//log.Info("response Body: %v", metadata["name"])

			servURL = "https://" + host + ":" + port + "/api/v1/namespaces/" + metadata["name"].(string) + "/services"
			servReq, err = http.NewRequest("GET", servURL, nil)
			servReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", string(token)))
			if err != nil {
				log.Fatalf("can't connect to the apiserver: %v", err)
			}
			//	servClient = &http.Client{}
			servResp, err = client.Do(servReq)
			if err != nil {
				log.Fatalf("error in sending the request: %v", err)
			}
			body, _ = ioutil.ReadAll(servResp.Body)
			servResp.Body.Close()

			var data1 map[string]interface{}
			err = json.Unmarshal(body, &data1)
			services := data1["items"].([]interface{})
			for i := range services {
				service := services[i].(map[string]interface{})
				spec := service["spec"].(map[string]interface{})
				serviceMetadata := service["metadata"].(map[string]interface{})
				labels := serviceMetadata["labels"].(map[string]interface{})
				//log.Info("response Body: %v", labels["name"])
				//log.Info("response Body: %v", spec["clusterIP"])
				if labels["name"] != nil && labels["app"] != "deis" {
					setEtcd(etcdClient, "/registry/services/specs/"+metadata["name"].(string)+"/"+labels["name"].(string), spec["clusterIP"].(string), ttl)
				}
			}
		}
		time.Sleep(timeout)
	}
}

func launchCron() {
	// edit crontab
	crontab := `(echo "* * * * * generate-certs >> /dev/stdout") | crontab -`
	cmd := exec.Command("bash", "-c", crontab)
	if err := cmd.Run(); err != nil {
		log.Fatalf("could not write to crontab: %v", err)
	}

	// run cron
	cmd = exec.Command("crond")
	if err := cmd.Run(); err != nil {
		log.Fatalf("cron terminated by error: %v", err)
	}
}

// Wait until the compilation of the templates
func waitForInitialConfd(etcd string, timeout time.Duration) {
	for {
		var buffer bytes.Buffer
		output := bufio.NewWriter(&buffer)
		log.Debugf("Connecting to etcd server %s", etcd)
		cmd := exec.Command("confd", "-node", etcd, "-onetime", "--log-level", "error")
		cmd.Stdout = output
		cmd.Stderr = output

		err := cmd.Run()
		output.Flush()
		if err == nil {
			break
		}

		log.Info("waiting for confd to write initial templates...")
		log.Debugf("\n%s", buffer.String())
		time.Sleep(timeout)
	}
}

func launchConfd(etcd string) {
	cmd := exec.Command("confd", "-node", etcd, "--log-level", "error", "--interval", "5")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Warn("confd terminated by error: %v", err)
	}
}

func launchNginx(nginxChan chan bool) {
	cmd := exec.Command("/opt/nginx/sbin/nginx", "-c", "/opt/nginx/conf/nginx.conf")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Warn("Nginx terminated by error: %v", err)
	}

	// Wait until the nginx is available
	for {
		_, err := net.DialTimeout("tcp", "127.0.0.1:80", timeout)
		if err == nil {
			nginxChan <- true
			break
		}
	}

	if err := cmd.Wait(); err != nil {
		log.Warnf("Nginx terminated by error: %v", err)
	} else {
		log.Info("Reloading nginx (change in configuration)...")
	}
}

func mkfifo(path string) {
	os.Remove(path)
	if err := syscall.Mkfifo(path, syscall.S_IFIFO|0666); err != nil {
		log.Fatalf("%v", err)
	}
}

func publishService(
	client *etcd.Client,
	etcdPath string,
	host string,
	externalPort string,
	ttl uint64) {

	for {
		setEtcd(client, etcdPath, host+":"+externalPort, ttl)
		time.Sleep(timeout)
	}
}

func setEtcd(client *etcd.Client, key, value string, ttl uint64) {
	_, err := client.Set(key, value, ttl)
	if err != nil {
		log.Warn(err)
	}
}

func setDefaultEtcd(client *etcd.Client, key, value string) {
	_, err := client.Set(key, value, 0)
	if err != nil {
		log.Warn(err)
	}
}

func mkdirEtcd(client *etcd.Client, path string) {
	_, err := client.CreateDir(path, 0)
	if err != nil && !strings.Contains(err.Error(), "Key already exists") {
		log.Warn(err)
	}
}

func getopt(name, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
}

/*
func getHostIP(dfault string) string {
	f, err := os.Open("/etc/environment")
	if err != nil {
		log.Println(err)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		s := strings.Split(line, "=")
		name, ip := s[0], s[1]
		if name == "COREOS_PRIVATE_IPV4" {
			return ip
		}
	}
	return dfault
}*/
