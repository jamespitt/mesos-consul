package mesos

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/jamespitt/mesos-consul/config"
	"github.com/jamespitt/mesos-consul/consul"
	"github.com/jamespitt/mesos-consul/registry"
	"github.com/jamespitt/mesos-consul/state"

	proto "github.com/mesos/mesos-go/api/v0/mesosproto"
	log "github.com/sirupsen/logrus"
)

type Mesos struct {
	Registry registry.Registry
	Agents   map[string]string
	Lock     sync.Mutex

	Leader    *proto.MasterInfo
	Masters   []*proto.MasterInfo
	started   sync.Once
	startChan chan struct{}

	IpOrder []string
	taskTag map[string][]string

	// Whitelist/Blacklist privileges
	TaskPrivilege *Privilege
	FwPrivilege   *Privilege

	Separator string

	ServiceName      string
	ServiceTags      []string
	ServiceIdPrefix  string
	ServicePortLabel string
}

func New(config *config.Config) *Mesos {
	mesos := new(Mesos)

	if config.Zk == "" {
		return nil
	}
	mesos.Separator = config.Separator

	mesos.TaskPrivilege = NewPrivilege(config.TaskWhiteList, config.TaskBlackList)
	mesos.FwPrivilege = NewPrivilege(config.FwWhiteList, config.FwBlackList)

	var err error
	mesos.taskTag, err = buildTaskTag(config.TaskTag)
	if err != nil {
		log.WithField("task-tag", config.TaskTag).Fatal(err.Error())
	}

	mesos.ServiceName = cleanName(config.ServiceName, config.Separator)

	mesos.Registry = consul.New()

	if mesos.Registry == nil {
		log.Fatal("No registry specified")
	}

	mesos.zkDetector(config.Zk)

	mesos.IpOrder = strings.Split(config.MesosIpOrder, ",")
	for _, src := range mesos.IpOrder {
		switch src {
		case "netinfo", "host", "docker", "mesos":
		default:
			log.Fatalf("Invalid IP Search Order: '%v'", src)
		}
	}
	log.Debugf("mesos.IpOrder = '%v'", mesos.IpOrder)

	if config.ServiceTags != "" {
		mesos.ServiceTags = strings.Split(config.ServiceTags, ",")
	}

	mesos.ServiceIdPrefix = config.ServiceIdPrefix
	mesos.ServicePortLabel = config.ServicePortLabel

	return mesos
}

// buildTaskTag takes a slice of task-tag arguments from the command line
// and returns a map of tasks name patterns to slice of tags that should be applied.
func buildTaskTag(taskTag []string) (map[string][]string, error) {
	result := make(map[string][]string)

	for _, tt := range taskTag {
		parts := strings.Split(tt, ":")
		if len(parts) != 2 {
			return nil, errors.New("task-tag pattern invalid, must include 1 colon separator")
		}

		taskName := strings.ToLower(parts[0])
		log.WithField("task-tag", taskName).Debug("Using task-tag pattern")
		tags := strings.Split(parts[1], ",")

		if _, ok := result[taskName]; !ok {
			result[taskName] = tags
		} else {
			result[taskName] = append(result[taskName], tags...)
		}
	}

	return result, nil
}

func (mesos *Mesos) Refresh() error {
	state, err := mesos.loadState()
	if err != nil {
		log.Warn("state failed: ", err.Error())
		return err
	}

	if state.Leader == "" {
		return errors.New("Empty master")
	}

	if mesos.Registry.CacheCreate() {
		log.Info("Load cache... ")
		mesos.LoadCache()
	}

	mesos.parseState(state)

	return nil
}

func (mesos *Mesos) loadState() (state.State, error) {
	var err error
	var state state.State

	log.Debug("loadState() called")

	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New("can't connect to Mesos")
		}
	}()

	mh := mesos.getLeader()
	if mh.Ip == "" {
		log.Warn("No master in zookeeper")
		return state, errors.New("No master in zookeeper")
	}

	log.Debugf("Zookeeper leader: %s:%s", mh.Ip, mh.PortString)

	log.Debugf("reloading from master ", mh.Ip)
	state, err = mesos.loadFromMaster(mh.Ip, mh.PortString)

	if rip := leaderIP(state.Leader); rip != mh.Ip {
		log.Warn("master changed to ", rip)
		state, err = mesos.loadFromMaster(rip, mh.PortString)
	}

	return state, err
}

func (mesos *Mesos) loadFromMaster(ip string, port string) (state state.State, err error) {
	url := "http://" + ip + ":" + port + "/master/state.json"

	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = json.Unmarshal(body, &state)
	if err != nil {
		return
	}

	return state, nil
}

func (mesos *Mesos) parseState(state state.State) {
	log.Debug("Running parseState")

	mesos.RegisterHosts(state)
	log.Debug("Done running RegisterHosts")

	for _, framework:= range state.Frameworks {
		if !mesos.FwPrivilege.Allowed(framework.Name) {
      log.Debugf("not allowed with framework : (%v)", framework.Name)
			continue
		}
		for _, task := range framework.Tasks {
      log.Debugf("original TaskName (prereg...) : (%v)", task.Name)
			agent, ok := mesos.Agents[task.SlaveID]
			if ok && task.State == "TASK_RUNNING" {
				task.SlaveIP = agent
				mesos.registerTask(&task, agent)
			}
		}
	}

	mesos.Registry.Deregister()
}
