package cluster

import (
	"errors"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"github.com/wind-c/comqtt/v2/cluster/discovery"
	"github.com/wind-c/comqtt/v2/cluster/log"
	"github.com/wind-c/comqtt/v2/cluster/message"
	"github.com/wind-c/comqtt/v2/cluster/utils"
	"github.com/wind-c/comqtt/v2/config"
	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

func TestCluster(t *testing.T) {
	log.Init(log.DefaultOptions())

	bindPort1, err := utils.GetFreePort()
	require.NoError(t, err, "Failed to get free port for node1")
	raftPort1, err := utils.GetFreePort()
	require.NoError(t, err, "Failed to get free port for node1 Raft")

	bindPort2, err := utils.GetFreePort()
	require.NoError(t, err, "Failed to get free port for node2")
	raftPort2, err := utils.GetFreePort()
	require.NoError(t, err, "Failed to get free port for node2 Raft")

	bindPort3, err := utils.GetFreePort()
	require.NoError(t, err, "Failed to get free port for node3")
	raftPort3, err := utils.GetFreePort()
	require.NoError(t, err, "Failed to get free port for node3 Raft")

	members := []string{
		"127.0.0.1:" + strconv.Itoa(bindPort1),
		"127.0.0.1:" + strconv.Itoa(bindPort2),
		"127.0.0.1:" + strconv.Itoa(bindPort3),
	}

	conf1 := &config.Cluster{
		NodeName:      "node1",
		RaftImpl:      config.RaftImplHashicorp,
		BindAddr:      "127.0.0.1",
		BindPort:      bindPort1,
		RaftPort:      raftPort1,
		RaftBootstrap: true,
		RaftDir:       t.TempDir(),
		GrpcEnable:    false,
		Members:       members,
		DiscoveryWay:  config.DiscoveryWaySerf,
		NodesFileDir:  t.TempDir(),
	}
	agent1 := NewAgent(conf1)
	err = agent1.Start()
	require.NoError(t, err, "Agent start failed for node: %s", conf1.NodeName)

	conf2 := &config.Cluster{
		NodeName:      "node2",
		RaftImpl:      config.RaftImplHashicorp,
		BindAddr:      "127.0.0.1",
		BindPort:      bindPort2,
		RaftPort:      raftPort2,
		RaftBootstrap: false,
		RaftDir:       t.TempDir(),
		GrpcEnable:    false,
		Members:       members,
		DiscoveryWay:  config.DiscoveryWaySerf,
		NodesFileDir:  t.TempDir(),
	}
	agent2 := NewAgent(conf2)
	err = agent2.Start()
	defer agent2.Stop()
	require.NoError(t, err, "Agent start failed for node: %s", conf2.NodeName)

	conf3 := &config.Cluster{
		NodeName:      "node3",
		RaftImpl:      config.RaftImplHashicorp,
		BindAddr:      "127.0.0.1",
		BindPort:      bindPort3,
		RaftPort:      raftPort3,
		RaftBootstrap: false,
		RaftDir:       t.TempDir(),
		GrpcEnable:    false,
		Members:       members,
		DiscoveryWay:  config.DiscoveryWaySerf,
		NodesFileDir:  t.TempDir(),
	}
	agent3 := NewAgent(conf3)
	err = agent3.Start()
	defer agent3.Stop()
	require.NoError(t, err, "Agent start failed for node: %s", conf3.NodeName)

	time.Sleep(5 * time.Second)

	_, leader1 := agent1.raftPeer.GetLeader()
	_, leader2 := agent2.raftPeer.GetLeader()
	_, leader3 := agent3.raftPeer.GetLeader()

	require.Equal(t, leader1, "node1")
	require.Equal(t, leader2, "node1")
	require.Equal(t, leader3, "node1")

	members1 := agent1.GetMemberList()
	members2 := agent2.GetMemberList()
	members3 := agent3.GetMemberList()

	require.Equal(t, len(members1), 3)
	require.Equal(t, len(members2), 3)
	require.Equal(t, len(members3), 3)

	// Stop agent1 and check new leader
	agent1.Stop()
	time.Sleep(5 * time.Second)

	_, newLeader2 := agent2.raftPeer.GetLeader()
	_, newLeader3 := agent3.raftPeer.GetLeader()

	// Check that either agent2 or agent3 becomes the new leader
	if newLeader2 == "node2" || newLeader2 == "node3" {
		require.Equal(t, newLeader2, newLeader3, "Leaders should be the same for agent2 and agent3")
	} else {
		require.Fail(t, "New leader should be either node2 or node3")
	}

	// Restart agent1 and verify it is a follower
	err = agent1.Start()
	require.NoError(t, err, "Agent restart failed for node: %s", conf1.NodeName)
	defer agent1.Stop()

	time.Sleep(5 * time.Second)

	_, leaderAfterRestart1 := agent1.raftPeer.GetLeader()
	_, leaderAfterRestart2 := agent2.raftPeer.GetLeader()
	_, leaderAfterRestart3 := agent3.raftPeer.GetLeader()

	require.Equal(t, leaderAfterRestart2, leaderAfterRestart1)
	require.Equal(t, leaderAfterRestart3, leaderAfterRestart1)

	require.NotEqual(t, leaderAfterRestart1, "node1", "After restart, node1 should not be the leader")

	t.Log("Test completed successfully")
}

// storedInflightHook counts the hook calls that would delete stored inflight rows.
type storedInflightHook struct {
	mqtt.HookBase
	calls atomic.Int32
}

func (h *storedInflightHook) ID() string { return "stored-inflight" }

func (h *storedInflightHook) Provides(b byte) bool {
	return b == mqtt.OnQosDropped || b == mqtt.OnQosComplete
}

func (h *storedInflightHook) OnQosDropped(cl *mqtt.Client, pk packets.Packet)  { h.calls.Add(1) }
func (h *storedInflightHook) OnQosComplete(cl *mqtt.Client, pk packets.Packet) { h.calls.Add(1) }

func TestProcessRelayMsgConnectRemovesClient(t *testing.T) {
	log.Init(log.DefaultOptions())

	s := mqtt.New(nil)
	hook := new(storedInflightHook)
	require.NoError(t, s.AddHook(hook, nil))
	a := &Agent{mqttServer: s}

	r, w := net.Pipe()
	t.Cleanup(func() { _ = w.Close() })
	go func() { _, _ = io.Copy(io.Discard, w) }()
	cl := s.NewClient(r, "t1", "zen", false)
	for id := uint16(1); id <= 2; id++ {
		cl.State.Inflight.Set(packets.Packet{PacketID: id, Created: time.Now().Unix()})
		atomic.AddInt64(&s.Info.Inflight, 1)
	}
	s.Clients.Add(cl)

	a.processRelayMsg(&message.Message{Type: packets.Connect, ClientID: "zen", NodeID: "other"})

	_, ok := s.Clients.Get("zen")
	require.False(t, ok)
	require.Equal(t, 0, cl.State.Inflight.Len())
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.Inflight))
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.InflightDropped))
	require.Equal(t, int32(0), hook.calls.Load())
}

// fakeOwner answers SessionOwner with a fixed result.
type fakeOwner struct {
	owner string
	err   error
}

func (f fakeOwner) SessionOwner(string) (string, error) { return f.owner, f.err }

func TestProcessRelayMsgConnectChecksOwner(t *testing.T) {
	log.Init(log.DefaultOptions())

	for name, tc := range map[string]struct {
		owners SessionOwner
		kept   bool
	}{
		"owned here":    {owners: fakeOwner{owner: "c01"}, kept: true},
		"owned by peer": {owners: fakeOwner{owner: "c02"}},
		"no owner":      {owners: fakeOwner{}},
		"lookup failed": {owners: fakeOwner{err: errors.New("redis down")}},
	} {
		t.Run(name, func(t *testing.T) {
			s := mqtt.New(nil)
			a := &Agent{mqttServer: s, Config: &config.Cluster{NodeName: "c01"}}
			a.SetSessionOwner(tc.owners)

			r, w := net.Pipe()
			t.Cleanup(func() { _ = w.Close() })
			go func() { _, _ = io.Copy(io.Discard, w) }()
			s.Clients.Add(s.NewClient(r, "t1", "zen", false))

			a.processRelayMsg(&message.Message{Type: packets.Connect, ClientID: "zen", NodeID: "c02"})

			_, ok := s.Clients.Get("zen")
			require.Equal(t, tc.kept, ok)
		})
	}
}

// sentNode records SendToOthers; the embedded nil Node panics on anything else.
type sentNode struct {
	discovery.Node
	sent chan []byte
}

func (n *sentNode) SendToOthers(msg []byte) { n.sent <- msg }

func TestMqttEventHookNotifiesEverySession(t *testing.T) {
	log.Init(log.DefaultOptions())

	for name, cl := range map[string]*mqtt.Client{
		"new":    {ID: "zen", InheritWay: mqtt.InheritWayNew},
		"local":  {ID: "zen", InheritWay: mqtt.InheritWayLocal},
		"remote": {ID: "zen", InheritWay: mqtt.InheritWayRemote},
	} {
		t.Run(name, func(t *testing.T) {
			pool, err := ants.NewPool(1)
			require.NoError(t, err)
			defer pool.Release()

			node := &sentNode{sent: make(chan []byte, 1)}
			a := &Agent{Config: &config.Cluster{NodeName: "c01"}, OutPool: pool, membership: node}
			h := &MqttEventHook{agent: a}

			h.OnSessionEstablished(cl, packets.Packet{Connect: packets.ConnectParams{Clean: true}})

			select {
			case b := <-node.sent:
				var m message.Message
				require.NoError(t, m.MsgpackLoad(b))
				require.Equal(t, packets.Connect, m.Type)
				require.Equal(t, "zen", m.ClientID)
				require.Equal(t, "c01", m.NodeID)
			case <-time.After(time.Second):
				t.Fatal("no takeover notice sent")
			}
		})
	}
}
