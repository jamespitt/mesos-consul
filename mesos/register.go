package mesos

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jamespitt/mesos-consul/registry"
	"github.com/jamespitt/mesos-consul/state"
	log "github.com/sirupsen/logrus"
)

// Query the consul agent on the Mesos Master
// to initialize the cache.
//
// All services created by mesos-consul are prefixed
// with service-id-prefix flag, followed by a colon.
//
func (mesos *Mesos) LoadCache() error {
	log.Info("Populating cache from Consul")

	mh := mesos.getLeader()

	return mesos.Registry.CacheLoad(mh.Ip, mesos.ServiceIdPrefix)
}

func (mesos *Mesos) RegisterHosts(s state.State) {
	log.Debug("Running RegisterHosts")

	mesos.Agents = make(map[string]string)

	// Register slaves
	for _, f := range s.Slaves {
		agent := toIP(f.PID.Host)
		port := toPort(f.PID.Port)

		mesos.Agents[f.ID] = agent

		mesos.registerHost(&registry.Service{
			ID:      fmt.Sprintf("%s:%s:%s:%s", mesos.ServiceIdPrefix, mesos.ServiceName, f.ID, f.Hostname),
			Name:    mesos.ServiceName,
			Port:    port,
			Address: agent,
			Agent:   agent,
			Tags:    mesos.agentTags("agent", "follower"),
			Check: &registry.Check{
				HTTP:     fmt.Sprintf("http://%s:%d/slave(1)/health", agent, port),
				Interval: "10s",
			},
		})
	}

	// Register masters
	mas := mesos.getMasters()
	for _, ma := range mas {
		var tags []string

		if ma.IsLeader {
			tags = mesos.agentTags("leader", "master")
		} else {
			tags = mesos.agentTags("master")
		}
		s := &registry.Service{
			ID:      fmt.Sprintf("%s:%s:%s:%s", mesos.ServiceIdPrefix, mesos.ServiceName, ma.Ip, ma.PortString),
			Name:    mesos.ServiceName,
			Port:    ma.Port,
			Address: ma.Ip,
			Agent:   ma.Ip,
			Tags:    tags,
			Check: &registry.Check{
				HTTP:     fmt.Sprintf("http://%s:%d/master/health", ma.Ip, ma.Port),
				Interval: "10s",
			},
		}

		mesos.registerHost(s)
	}
}

func (mesos *Mesos) registerHost(s *registry.Service) {
	h := mesos.Registry.CacheLookup(s.ID)
	if h != nil {
		if (h.ID == s.ID) {
		  if sliceEq(s.Tags, h.Tags) {
        mesos.Registry.CacheMark(s.ID)
        // Tags are the same. Return
        return
		  }
		}

		log.Debugf("Host found. Comparing tags: (%v, %v)", h.Tags, s.Tags)
		log.Debugf("Host ID: (%v, %v)", h.ID, s.ID)
		log.Debugf("Tags changed. Re-registering")

		// Delete cache entry. It will be re-created below
		mesos.Registry.CacheDelete(s.ID)
	}

	mesos.Registry.Register(s)
}

func (mesos *Mesos) registerTask(task *state.Task, agent string) {
	var tags []string
	var IDs map[string]string
  var id string

  IDs = make(map[string]string)

	registered := false

	taskName := cleanName(task.Name, mesos.Separator)
	log.Debugf("original TaskName : (%v)", taskName)
	if task.Label("overrideTaskName") != "" {
		taskName = cleanName(task.Label("overrideTaskName"), mesos.Separator)
		log.Debug("overrideTaskName to : (%v)", taskName)
	}
	if !mesos.TaskPrivilege.Allowed(taskName) {
		// Task not allowed to be registered
		return
	}

	address := task.IP(mesos.IpOrder...)
  log.Debug("task info ---  %+v\n", task)

	// build a map to indicate public ports
	var registerPorts map[int]struct{}
	if mesos.ServicePortLabel != "" {
		servicePortLabel := task.Label(mesos.ServicePortLabel)
		if servicePortLabel != "" {
			servicePortLabelArray := strings.Split(servicePortLabel, ",")
			if len(servicePortLabelArray) > 0 {
				registerPorts = make(map[int]struct{}, 0)
				for _, pv := range servicePortLabelArray {
					pv = strings.TrimSpace(pv)
					pi, err := strconv.Atoi(pv)
					if err == nil {
						registerPorts[pi] = struct{}{}
					}
				}
			}
		}
	}

	taskLabel := task.Label("tags")
	if taskLabel != "" {
		tags = strings.Split(task.Label("tags"), ",")
	} else {
		tags = []string{}
	}

	tags = buildRegisterTaskTags(taskName, tags, mesos.taskTag)

	for key := range task.DiscoveryInfo.Ports.DiscoveryPorts {
		// We append -portN to ports after the first.
		// This is done to preserve compatibility with
		// existing implementations which may rely on the
		// old unprefixed name.
		svcName := taskName
		if key > 0 {
			svcName = fmt.Sprintf("%s-port%d", svcName, key+1)
		}
		var porttags []string
		discoveryPort := state.DiscoveryPort(task.DiscoveryInfo.Ports.DiscoveryPorts[key])
		serviceName := discoveryPort.Name
		servicePort := strconv.Itoa(discoveryPort.Number)
		log.Debugf("%+v framework has %+v as a name for %+v port",
			task.Name,
			discoveryPort.Name,
			discoveryPort.Number)
		pl := discoveryPort.Label("tags")
		if pl != "" {
			porttags = strings.Split(discoveryPort.Label("tags"), ",")
		} else {
			porttags = []string{}
		}
		if discoveryPort.Name != "" {
			id = fmt.Sprintf("%s:%s:%s:%s:%d", mesos.ServiceIdPrefix, agent, taskName, address, discoveryPort.Number)
      IDs[id] = "set"
      log.Debugf("ID = %+v with name =  %+v ",id, discoveryPort.Name )

			mesos.Registry.Register(&registry.Service{
				ID:      fmt.Sprintf("%s:%s:%s:%s:%s:%d", mesos.ServiceIdPrefix, agent, taskName, address, discoveryPort.Name, discoveryPort.Number),
				Name:    taskName,
				Port:    toPort(servicePort),
				Address: address,
				Tags:    append(append(tags, serviceName), porttags...),
				Check: GetCheck(task, &CheckVar{
					Host: toIP(address),
					Port: servicePort,
				}),
				Agent: toIP(agent),
			})
			registered = true
		}
	}


	if task.Resources.PortRanges != "" {
		for key, port := range task.Resources.Ports() {
			// do not register port if explicit port label was found
			if _, ok := registerPorts[key]; len(registerPorts) > 0 && !ok {
				continue
			}

      // We use this as a check to see if we've already got this setup...
			id = fmt.Sprintf("%s:%s:%s:%s:%s", mesos.ServiceIdPrefix, agent, taskName, address, port)
      log.Debugf("ID = %+v withoutname, in range ",id)
      if  _, ok := IDs[id]; ok  {
        log.Debugf("%+v already has %+v port - leaving",
          taskName,
          port)
				continue
			}

			// We append -portN to ports after the first.
			// This is done to preserve compatibility with
			// existing implementations which may rely on the
			// old unprefixed name.
			svcName := taskName
			if key > 0 {
				svcName = fmt.Sprintf("%s-port%d", svcName, key+1)
			}

			log.Debugf("%+v framework has %+v as a name for %+v port",
				task.Name,
				svcName,
				port)


      mesos.Registry.Register(&registry.Service{
        ID:      fmt.Sprintf("%s:%s:%s:%s:%s:%s", mesos.ServiceIdPrefix, agent, taskName, address, svcName, port),
				Name:    taskName,
				Port:    toPort(port),
				Address: address,
				Tags:    tags,
				Check: GetCheck(task, &CheckVar{
					Host: toIP(address),
					Port: port,
				}),
				Agent: toIP(agent),
			})
			registered = true
		}
	}

	if !registered {
		mesos.Registry.Register(&registry.Service{
			ID:      fmt.Sprintf("%s:%s-%s:%s", mesos.ServiceIdPrefix, agent, taskName, address),
			Name:    taskName,
			Address: address,
			Tags:    tags,
			Check: GetCheck(task, &CheckVar{
				Host: toIP(address),
			}),
			Agent: toIP(agent),
		})
	}
}

// buildRegisterTaskTags takes a cleaned task name, a slice of starting tags, and the processed
// taskTag map and returns a slice of tags that should be applied to this task.
func buildRegisterTaskTags(taskName string, startingTags []string, taskTag map[string][]string) []string {
	result := startingTags
	tnameLower := strings.ToLower(taskName)

	for pattern, taskTags := range taskTag {
		for _, tag := range taskTags {
			if strings.Contains(tnameLower, pattern) {
				if !sliceContainsString(result, tag) {
					log.WithField("task-tag", tnameLower).Debug("Task matches pattern for tag")
					result = append(result, tag)
				}
			}
		}
	}

	return result
}

func (mesos *Mesos) agentTags(ts ...string) []string {
	if len(mesos.ServiceTags) == 0 {
		return ts
	}

	rval := []string{}

	for _, tag := range mesos.ServiceTags {
		for _, t := range ts {
			rval = append(rval, fmt.Sprintf("%s.%s", t, tag))
		}
	}

	return rval
}
