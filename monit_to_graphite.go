package main

import (
    "bytes"
    "encoding/xml"
    "errors"
    "flag"
    "fmt"
    "log"
    "net"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

var ErrHelp = errors.New("flag: help requested")
var Usage = func() {
    fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
    flag.PrintDefaults()
}

type Server struct {
    Uptime        int    `xml:"uptime"`
    Poll          int    `xml:"poll"`
    Localhostname string `xml:"localhostname"`
}

type Platform struct {
    Name    string `xml:"name"`
    Release string `xml:"release"`
    Version string `xml:"version"`
    Machine string `xml:"machine"`
    Cpu     int    `xml:"cpu"`
    Memory  int    `xml:"memory"`
    Swap    int    `xml:"swap"`
}

type Memory struct {
    Percent       float64 `xml:"percent"`
    Percenttotal  float64 `xml:"percenttotal"`
    Kilobyte      int     `xml:"kylobyte"`
    Kilobytetotal int     `xml:"kilobytetotal"`
}

type Cpu struct {
    Percent      float64 `xml:"percent"`
    Percenttotal float64 `xml:"percenttotal"`
}

type Load struct {
    Avg01 float64 `xml:"avg01"`
    Avg05 float64 `xml:"avg05"`
    Avg15 float64 `xml:"avg15"`
}

type Cpusys struct {
    User   float64 `xml:"user"`
    System float64 `xml:"system"`
    Wait   float64 `xml:"wait"`
}

type System struct {
    Load   Load   `xml:"load"`
    Cpusys Cpusys `xml:"cpu"`
    Memory Memory `xml:"memory"`
}

type Block struct {
    Percent float64 `xml:"percent"`
    Usage   float64 `xml:"usage"`
    Total   float64 `xml:"total"`
}

type Inode struct {
    Percent float64 `xml:"percent"`
    Usage   float64 `xml:"usage"`
    Total   float64 `xml:"total"`
}

type Service struct {
    Prefix        string
    Collected_Sec int64  `xml:"collected_sec"`
    Type          int    `xml:"type"`
    Name          string `xml:"name,attr"`
    Status        int    `xml:"status"`
    Monitor       int    `xml:"monitor"`
    MonitorMode   int    `xml:"monitormode"`
    Pendingaction int    `xml:"pendingaction"`
    Pid           int    `xml:"pid"`
    Ppid          int    `xml:"ppid"`
    Uptime        int    `xml:"uptime"`
    Children      int    `xml:"children"`
    Memory        Memory `xml:"memory"`
    Cpu           Cpu    `xml:"cpu"`
    System        System `xml:"system"`
    Block         Block  `xml:"block"`
    Inode         Inode  `xml:"inode"`
}

type Monit struct {
    Id          string    `xml:"id,attr"`
    Incarnation int       `xml:"incarnation,attr"`
    Version     string    `xml:"version,attr"`
    XMLName     xml.Name  `xml:"monit"`
    Server      Server    `xml:"server"`
    Platform    Platform  `xml:"platform"`
    Service     []Service `xml:"services>service"`
}

type Graphite struct {
    addr string
}

const (
    MonitTypeFileSystem int = 0
    MonitTypeDirectory      = 1
    MonitTypeFile           = 2
    MonitTypeProcess        = 3
    MonitTypeSystem         = 5
    MonitTypeProgram        = 7
)

type Metric struct {
    metric    string
    value     string
    timestamp int64
}

func ProcessServices(serviceq chan Service, metricq chan Metric) {
    for {
        service := <-serviceq

        switch service.Type {
        case MonitTypeSystem:
            service.Name = "system"

            metricq <- Metric{service.Prefix + "." + service.Name + ".cpu.user", strconv.FormatFloat(service.System.Cpusys.User, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".cpu.system", strconv.FormatFloat(service.System.Cpusys.System, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".cpu.wait", strconv.FormatFloat(service.System.Cpusys.Wait, 'g', -1, 64), service.Collected_Sec}

            metricq <- Metric{service.Prefix + "." + service.Name + ".load.avg01", strconv.FormatFloat(service.System.Load.Avg01, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".load.avg05", strconv.FormatFloat(service.System.Load.Avg05, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".load.avg15", strconv.FormatFloat(service.System.Load.Avg15, 'g', -1, 64), service.Collected_Sec}

            metricq <- Metric{service.Prefix + "." + service.Name + ".memory.percent", strconv.FormatFloat(service.System.Memory.Percent, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".memory.percenttotal", strconv.FormatFloat(service.System.Memory.Percenttotal, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".memory.kilobyte", strconv.Itoa(service.System.Memory.Kilobyte), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".memory.kilobytetotal", strconv.Itoa(service.System.Memory.Kilobytetotal), service.Collected_Sec}

        case MonitTypeProcess:
            /* Too verbose for us, will need to allow configuration to dictate which metrics to send
               service.Prefix = service.Prefix + ".process"

               metricq <- Metric{ service.Prefix+"."+service.Name+".status", strconv.Itoa(service.Status), service.Collected_Sec }
               metricq <- Metric{ service.Prefix+"."+service.Name+".monitor", strconv.Itoa(service.Monitor), service.Collected_Sec }
               metricq <- Metric{ service.Prefix+"."+service.Name+".uptime", strconv.Itoa(service.Uptime), service.Collected_Sec }
               metricq <- Metric{ service.Prefix+"."+service.Name+".children", strconv.Itoa(service.Children), service.Collected_Sec }

               metricq <- Metric{ service.Prefix+"."+service.Name+".memory.percent", strconv.FormatFloat(service.Memory.Percent, 'g', -1, 64), service.Collected_Sec }
               metricq <- Metric{ service.Prefix+"."+service.Name+".memory.percent_total", strconv.FormatFloat(service.Memory.Percenttotal, 'g', -1, 64), service.Collected_Sec }
               metricq <- Metric{ service.Prefix+"."+service.Name+".memory.kilobyte", strconv.Itoa(service.Memory.Kilobyte), service.Collected_Sec }
               metricq <- Metric{ service.Prefix+"."+service.Name+".memory.kylobytetotal", strconv.Itoa(service.Memory.Kilobytetotal), service.Collected_Sec }

               metricq <- Metric{ service.Prefix+"."+service.Name+".cpu.percent", strconv.FormatFloat(service.Cpu.Percent, 'g', -1, 64), service.Collected_Sec }
               metricq <- Metric{ service.Prefix+"."+service.Name+".cpu.percenttotal", strconv.FormatFloat(service.Cpu.Percenttotal, 'g', -1, 64), service.Collected_Sec }
            */

        case MonitTypeFileSystem:
            service.Prefix = service.Prefix + ".filesystem"

            metricq <- Metric{service.Prefix + "." + service.Name + ".block.percent", strconv.FormatFloat(service.Block.Percent, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".block.usage", strconv.FormatFloat(service.Block.Usage, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".block.total", strconv.FormatFloat(service.Block.Total, 'g', -1, 64), service.Collected_Sec}

            metricq <- Metric{service.Prefix + "." + service.Name + ".inode.percent", strconv.FormatFloat(service.Inode.Percent, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".inode.usage", strconv.FormatFloat(service.Inode.Usage, 'g', -1, 64), service.Collected_Sec}
            metricq <- Metric{service.Prefix + "." + service.Name + ".inode.total", strconv.FormatFloat(service.Inode.Total, 'g', -1, 64), service.Collected_Sec}

        default:
        }
    }
}

func ProcessMetrics(metricq chan Metric, graphite *Graphite) {
    ticker := time.Tick(60 * time.Second)
    metricm := map[string]Metric{}
    for {
        select {
        case <-ticker:
            if len(metricm) > 0 {
                metricm = SendMap(metricm, graphite)
            }
        case metric := <-metricq:
            // Overwrite duplicates to avoid iowait on carbon, we don't need them for monit's metrics
            metricm[metric.metric] = metric
        }
    }
}

func SendMap(metricm map[string]Metric, graphite *Graphite) map[string]Metric {
    count := 0
    buffer := bytes.NewBufferString("")
    for _, metric := range metricm {
        fmt.Fprintf(buffer, "monit.%s %s %d\n", metric.metric, metric.value, metric.timestamp)
        count++

        // Send no more than 500 metrics in a batch
        if count >= 500 {
            SendBuffer(buffer, graphite)
            log.Println("metrics sent to graphite:", count)
            count = 0
        }
    }

    if count > 0 {
        SendBuffer(buffer, graphite)
        log.Println("metrics sent to graphite:", count)
    }

    return map[string]Metric{}
}

func SendBuffer(buffer *bytes.Buffer, graphite *Graphite) {
    var conn net.Conn
    var err error
    for i := 0; i <= 5; i++ {
        conn, err = net.Dial("tcp", graphite.addr)
        if i == 5 {
            log.Fatal(err)
        }
        if conn == nil {
            switch err.(type) {
            default:
                log.Fatal(err)
            case *net.OpError:
                continue
            }
        } else {
            defer conn.Close()
            break
        }
    }
    conn.Write(buffer.Bytes())
    buffer.Reset()
}

var serviceq = make(chan Service)

func MonitServer(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    var monit Monit
    p := xml.NewDecoder(req.Body)

    if debug {
        b := new(bytes.Buffer)
        b.ReadFrom(req.Body)
        log.Fatal(b.String())
    }

    p.CharsetReader = CharsetReader
    err := p.DecodeElement(&monit, nil)
    if err != nil {
        log.Fatal(err)
    }

    // log.Println("Got message from", monit.Server.Localhostname)

    var shortname string
    i := strings.Index(monit.Server.Localhostname, ".")
    if i != -1 {
        shortname = monit.Server.Localhostname[:i]
    } else {
        shortname = monit.Server.Localhostname
    }

    for _, service := range monit.Service {
        service.Prefix = shortname
        serviceq <- service
    }
}

var debug = false

func main() {
    flag.Parse()
    var carbonAddress *string = flag.String("c", "127.0.0.1:2003", "carbon address")
    var forwarderAddress *string = flag.String("l", ":3005", "forwarder listening address")
    flag.BoolVar(&debug, "d", false, "print the first m/monit xml message and exit")

    log.Println("Forwarding m/monit to ", *carbonAddress)

    graphite := &Graphite{addr: *carbonAddress}
    metricq := make(chan Metric)

    go ProcessServices(serviceq, metricq)
    go ProcessMetrics(metricq, graphite)

    http.HandleFunc("/collector", MonitServer)
    log.Println("Forwarder listening input on: ", *forwarderAddress)
    err := http.ListenAndServe(*forwarderAddress, nil)
    if err != nil {
        log.Fatal("ListenAndServe: ", err)
    }
}
