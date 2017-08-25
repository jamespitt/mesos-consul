package mesos

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/mesos/mesos-go/api/v0/detector"
	_ "github.com/mesos/mesos-go/api/v0/detector/zoo"
	proto "github.com/mesos/mesos-go/api/v0/mesosproto"
	log "github.com/sirupsen/logrus"
)

func (mesos *Mesos) OnMasterChanged(leader *proto.MasterInfo) {
	mesos.Lock.Lock()
	defer mesos.Lock.Unlock()

	mesos.started.Do(func() { close(mesos.startChan) })

	mesos.Leader = leader
}

func (mesos *Mesos) UpdatedMasters(masters []*proto.MasterInfo) {
	mesos.Lock.Lock()
	defer mesos.Lock.Unlock()

	mesos.Masters = masters
}

func (mesos *Mesos) zkDetector(zkURI string) {
	if zkURI == "" {
		log.Fatal("Zookeeper address not provided")
	}

	log.WithField("zk", zkURI).Debug("Zookeeper address")
	md, err := detector.New(zkURI)
	if err != nil {
		log.Fatal(err.Error())
	}

	mesos.startChan = make(chan struct{})
	md.Detect(mesos)

	select {
	case <-mesos.startChan:
		log.Info("Done waiting for initial leader information from Zookeeper.")
	case <-time.After(2 * time.Minute):
		log.Fatal("Timed out waiting for initial ZK detection.")
	}
}

// Get the leader out of the list of masters
//
func (mesos *Mesos) getLeader() *MesosHost {
	mesos.Lock.Lock()
	defer mesos.Lock.Unlock()

	return MasterInfoToMesosHost(mesos.Leader)
}

func (mesos *Mesos) getMasters() []*MesosHost {
	mesos.Lock.Lock()
	defer mesos.Lock.Unlock()

	ms := make([]*MesosHost, len(mesos.Masters))
	for i, msp := range mesos.Masters {
		mh := MasterInfoToMesosHost(msp)
		if *mesos.Leader.Id == *msp.Id {
			mh.IsLeader = true
		}

		ms[i] = mh
	}
	return ms
}

func MasterInfoToMesosHost(mi *proto.MasterInfo) *MesosHost {
	if mi == nil {
		return &MesosHost{
			Host:         "",
			Ip:           "",
			Port:         0,
			PortString:   "",
			IsLeader:     false,
			IsRegistered: false,
		}
	}

	addr := mi.GetAddress()
	if addr.GetHostname() != "" {
		return &MesosHost{
			Host:         addr.GetHostname(),
			Ip:           addr.GetIp(),
			Port:         int(addr.GetPort()),
			PortString:   fmt.Sprintf("%d", addr.GetPort()),
			IsLeader:     false,
			IsRegistered: false,
		}
	} else {
		log.Debug("Using old protobuf format")
		// Old protobuf format
		return ProtoBufToMesosHost(mi)
	}
}

func ProtoBufToMesosHost(mi *proto.MasterInfo) *MesosHost {
	ipstring := ""
	port := ""

	log.WithField("mi.GetHostname()", mi.GetHostname()).Debug("protobuf MasterInfo")
	log.WithField("mi.GetIp()", packedIpToString(mi.GetIp())).Debug("protobuf MasterInfo")
	log.WithField("mi.GetPort()", fmt.Sprint(mi.GetPort())).Debug("protobuf MasterInfo")

	if host := mi.GetHostname(); host != "" {
		if ip, err := net.LookupIP(host); err == nil {
			for _, i := range ip {
				if four := i.To4(); four != nil {
					ipstring = i.String()
					break
				}
			}
		}
	}

	if ipstring == "" {
		ipstring = packedIpToString(mi.GetIp())
	}

	if ipstring == "" {
		ipstring = mi.GetHostname()
	}

	if len(ipstring) > 0 {
		port = fmt.Sprint(mi.GetPort())
	}

	return &MesosHost{
		Host:         mi.GetHostname(),
		Ip:           ipstring,
		Port:         int(mi.GetPort()),
		PortString:   port,
		IsLeader:     false,
		IsRegistered: false,
	}
}

func packedIpToString(p uint32) string {
	octets := make([]byte, 4, 4)
	binary.LittleEndian.PutUint32(octets, p)
	ipv4 := net.IP(octets)
	return ipv4.String()
}
