// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package bolt

import (
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/hooks/storage"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
	"github.com/wind-c/comqtt/v2/mqtt/system"

	"github.com/asdine/storm/v3"
	"github.com/stretchr/testify/require"
)

var (
	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

	client = &mqtt.Client{
		ID: "test",
		Net: mqtt.ClientConnection{
			Remote:   "test.addr",
			Listener: "listener",
		},
		Properties: mqtt.ClientProperties{
			Username: []byte("username"),
			Clean:    false,
		},
	}

	pkf = packets.Packet{Filters: packets.Subscriptions{{Filter: "a/b/c"}}}
)

func teardown(t *testing.T, path string, h *Hook) {
	_ = h.Stop()
	err := os.Remove(path)
	require.NoError(t, err)
}

func TestClientKey(t *testing.T) {
	k := clientKey(&mqtt.Client{ID: "cl1"})
	require.Equal(t, "cl1", k)
}

func TestSubscriptionKey(t *testing.T) {
	k := subscriptionKey(&mqtt.Client{ID: "cl1"}, "a/b/c")
	require.Equal(t, storage.SubscriptionKey+"_cl1:a/b/c", k)
}

func TestRetainedKey(t *testing.T) {
	k := retainedKey("a/b/c")
	require.Equal(t, storage.RetainedKey+"_a/b/c", k)
}

func TestInflightKey(t *testing.T) {
	k := inflightKey(&mqtt.Client{ID: "cl1"}, packets.Packet{PacketID: 1})
	require.Equal(t, storage.InflightKey+"_cl1:1", k)
}

func TestSysInfoKey(t *testing.T) {
	require.Equal(t, storage.SysInfoKey, sysInfoKey())
}

func TestID(t *testing.T) {
	h := new(Hook)
	require.Equal(t, "bolt-db", h.ID())
}

func TestProvides(t *testing.T) {
	h := new(Hook)
	require.True(t, h.Provides(mqtt.OnSessionEstablish))
	require.True(t, h.Provides(mqtt.OnSessionEstablished))
	require.True(t, h.Provides(mqtt.OnDisconnect))
	require.True(t, h.Provides(mqtt.OnSubscribed))
	require.True(t, h.Provides(mqtt.OnUnsubscribed))
	require.True(t, h.Provides(mqtt.OnRetainMessage))
	require.True(t, h.Provides(mqtt.OnQosPublish))
	require.True(t, h.Provides(mqtt.OnQosComplete))
	require.True(t, h.Provides(mqtt.OnQosDropped))
	require.True(t, h.Provides(mqtt.OnSysInfoTick))
	require.True(t, h.Provides(mqtt.OnSessionDiscarded))
	require.True(t, h.Provides(mqtt.StoredClients))
	require.True(t, h.Provides(mqtt.StoredInflightMessages))
	require.True(t, h.Provides(mqtt.StoredRetainedMessages))
	require.True(t, h.Provides(mqtt.StoredSubscriptions))
	require.True(t, h.Provides(mqtt.StoredSysInfo))
	require.False(t, h.Provides(mqtt.OnACLCheck))
	require.False(t, h.Provides(mqtt.OnConnectAuthenticate))
}

func TestInitBadConfig(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)

	err := h.Init(map[string]any{})
	require.Error(t, err)
}

func TestInitUseDefaults(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	require.Equal(t, defaultTimeout, h.config.Options.Timeout)
	require.Equal(t, defaultDbFile, h.config.Path)
}

func TestInitBadPath(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(&Options{
		Path: "..",
	})
	require.Error(t, err)
}

func TestOnSessionEstablishedThenOnDisconnect(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	h.OnSessionEstablished(client, packets.Packet{})

	r := new(storage.Client)
	err = h.db.One("ID", clientKey(client), r)
	require.NoError(t, err)
	require.Equal(t, client.ID, r.ID)
	require.Equal(t, client.Net.Remote, r.Remote)
	require.Equal(t, client.Net.Listener, r.Listener)
	require.Equal(t, client.Properties.Username, r.Username)
	require.Equal(t, client.Properties.Clean, r.Clean)
	require.NotSame(t, client, r)

	h.OnDisconnect(client, nil, false)
	r2 := new(storage.Client)
	err = h.db.One("ID", clientKey(client), r2)
	require.NoError(t, err)
	require.Equal(t, client.ID, r.ID)

	h.OnDisconnect(client, nil, true)
	r3 := new(storage.Client)
	err = h.db.One("ID", clientKey(client), r3)
	require.Error(t, err)
	require.ErrorIs(t, storm.ErrNotFound, err)
	require.Empty(t, r3.ID)
}

func TestOnSessionEstablishedSavesSessionExpiryFlag(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	cl := &mqtt.Client{ID: "v5", Properties: mqtt.ClientProperties{
		ProtocolVersion: 5,
		Props:           packets.Properties{SessionExpiryInterval: 300, SessionExpiryIntervalFlag: true},
	}}
	h.OnSessionEstablished(cl, packets.Packet{})

	r, err := h.StoredClients()
	require.NoError(t, err)
	require.Len(t, r, 1)
	require.Equal(t, uint32(300), r[0].Properties.SessionExpiryInterval)
	require.True(t, r[0].Properties.SessionExpiryIntervalFlag)
}

func TestOnSessionEstablishedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnSessionEstablished(client, packets.Packet{})
}

func TestOnSessionEstablishedClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnSessionEstablished(client, packets.Packet{})
}

func TestOnWillSent(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	c1 := client
	c1.Properties.Will.Flag = 1
	h.OnWillSent(c1, packets.Packet{})

	r := new(storage.Client)
	err = h.db.One("ID", clientKey(client), r)
	require.NoError(t, err)

	require.Equal(t, uint32(1), r.Will.Flag)
	require.NotSame(t, client, r)
}

func TestOnClientExpired(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	cl := &mqtt.Client{ID: "cl1"}
	clientKey := clientKey(cl)

	err = h.db.Save(&storage.Client{ID: cl.ID})
	require.NoError(t, err)

	r := new(storage.Client)
	err = h.db.One("ID", clientKey, r)
	require.NoError(t, err)
	require.Equal(t, cl.ID, r.ID)

	h.OnClientExpired(cl)
	err = h.db.One("ID", clientKey, r)
	require.Error(t, err)
	require.ErrorIs(t, storm.ErrNotFound, err)
}

func TestOnClientExpiredDeletesInflights(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	a := &mqtt.Client{ID: "a"}
	h.OnSessionEstablished(a, packets.Packet{})
	for i, cl := range []*mqtt.Client{a, a, {ID: "a:b"}, {ID: "zen"}} {
		h.OnQosPublish(cl, packets.Packet{PacketID: uint16(i + 1)}, time.Now().Unix(), 0)
	}

	h.OnClientExpired(a)

	r, err := h.StoredInflightMessages()
	require.NoError(t, err)
	got := []string{}
	for _, m := range r {
		got = append(got, m.ClientID)
	}
	require.ElementsMatch(t, []string{"a:b", "zen"}, got)

	err = h.db.One("ID", clientKey(a), new(storage.Client))
	require.ErrorIs(t, err, storm.ErrNotFound)
}

func TestOnSessionEstablishCleanDeletesInflights(t *testing.T) {
	for _, tc := range []struct {
		clean bool
		want  []string
	}{
		{clean: true, want: []string{"a:b", "zen"}},
		{clean: false, want: []string{"a", "a", "a:b", "zen"}},
	} {
		t.Run(strconv.FormatBool(tc.clean), func(t *testing.T) {
			h := new(Hook)
			h.SetOpts(logger, nil)
			err := h.Init(nil)
			require.NoError(t, err)
			defer teardown(t, h.config.Path, h)

			a := &mqtt.Client{ID: "a"}
			for i, cl := range []*mqtt.Client{a, a, {ID: "a:b"}, {ID: "zen"}} {
				h.OnQosPublish(cl, packets.Packet{PacketID: uint16(i + 1)}, time.Now().Unix(), 0)
			}

			h.OnSessionEstablish(a, packets.Packet{Connect: packets.ConnectParams{Clean: tc.clean}})

			r, err := h.StoredInflightMessages()
			require.NoError(t, err)
			got := []string{}
			for _, m := range r {
				got = append(got, m.ClientID)
			}
			require.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestOnClientExpiredClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnClientExpired(client)
}

func TestOnClientExpiredNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnClientExpired(client)
}

func TestOnSessionDiscarded(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	a, ab := &mqtt.Client{ID: "a"}, &mqtt.Client{ID: "a:b"}
	for i, cl := range []*mqtt.Client{a, ab} {
		h.OnSessionEstablished(cl, packets.Packet{})
		h.OnSubscribed(cl, packets.Packet{Filters: packets.Subscriptions{{Filter: "x/1"}, {Filter: "x/2"}}}, []byte{0, 0}, []int{1, 1})
		h.OnQosPublish(cl, packets.Packet{PacketID: uint16(i + 1)}, time.Now().Unix(), 0)
	}

	check := func() {
		err := h.db.One("ID", "a", new(storage.Client))
		require.ErrorIs(t, err, storm.ErrNotFound)
		require.NoError(t, h.db.One("ID", "a:b", new(storage.Client)))

		subs, err := h.StoredSubscriptions()
		require.NoError(t, err)
		got := []string{}
		for _, sub := range subs {
			got = append(got, sub.Client)
		}
		require.ElementsMatch(t, []string{"a:b", "a:b"}, got)

		msgs, err := h.StoredInflightMessages()
		require.NoError(t, err)
		got = []string{}
		for _, m := range msgs {
			got = append(got, m.ClientID)
		}
		require.ElementsMatch(t, []string{"a:b"}, got)
	}

	h.OnSessionDiscarded(a)
	check()

	h.OnSessionDiscarded(a)
	check()
}

func TestOnSessionDiscardedClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnSessionDiscarded(client)
}

func TestOnSessionDiscardedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnSessionDiscarded(client)
}

func TestOnDisconnectNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnDisconnect(client, nil, false)
}

func TestOnDisconnectClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnDisconnect(client, nil, false)
}

func TestOnDisconnectSessionTakenOver(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)

	testClient := &mqtt.Client{
		ID: "test",
		Net: mqtt.ClientConnection{
			Remote:   "test.addr",
			Listener: "listener",
		},
		Properties: mqtt.ClientProperties{
			Username: []byte("username"),
			Clean:    false,
		},
	}

	testClient.Stop(packets.ErrSessionTakenOver)
	teardown(t, h.config.Path, h)
	h.OnDisconnect(testClient, nil, true)
}

func TestOnSubscribedThenOnUnsubscribed(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	h.OnSubscribed(client, pkf, []byte{0}, nil)
	r := new(storage.Subscription)

	err = h.db.One("ID", subscriptionKey(client, pkf.Filters[0].Filter), r)
	require.NoError(t, err)
	require.Equal(t, client.ID, r.Client)
	require.Equal(t, pkf.Filters[0].Filter, r.Filter)
	require.Equal(t, byte(0), r.Qos)

	h.OnUnsubscribed(client, pkf, nil, nil)
	err = h.db.One("ID", subscriptionKey(client, pkf.Filters[0].Filter), r)
	require.Error(t, err)
	require.Equal(t, storm.ErrNotFound, err)
}

func TestOnSubscribedSkipsRefusedFilters(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	pk := packets.Packet{Filters: packets.Subscriptions{
		{Filter: "a/b/c"},
		{Filter: "d/e/f"},
		{Filter: "g/h/i"},
	}}
	h.OnSubscribed(client, pk, []byte{1, 0x80, 0x87}, nil)

	subs, err := h.StoredSubscriptions()
	require.NoError(t, err)
	require.Len(t, subs, 1)
	require.Equal(t, "a/b/c", subs[0].Filter)
}

func TestOnSubscribedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnSubscribed(client, pkf, []byte{0}, nil)
}

func TestOnSubscribedClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnSubscribed(client, pkf, []byte{0}, nil)
}

func TestOnUnsubscribedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnUnsubscribed(client, pkf, nil, nil)
}

func TestOnUnsubscribedClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnUnsubscribed(client, pkf, nil, nil)
}

func TestOnRetainMessageThenUnset(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Retain: true,
		},
		Payload:   []byte("hello"),
		TopicName: "a/b/c",
	}

	h.OnRetainMessage(client, pk, 1)

	r := new(storage.Message)
	err = h.db.One("ID", retainedKey(pk.TopicName), r)
	require.NoError(t, err)
	require.Equal(t, pk.TopicName, r.TopicName)
	require.Equal(t, pk.Payload, r.Payload)

	h.OnRetainMessage(client, pk, -1)
	err = h.db.One("ID", retainedKey(pk.TopicName), r)
	require.Error(t, err)
	require.Equal(t, storm.ErrNotFound, err)

	// coverage: delete deleted
	h.OnRetainMessage(client, pk, -1)
	err = h.db.One("ID", retainedKey(pk.TopicName), r)
	require.Error(t, err)
	require.Equal(t, storm.ErrNotFound, err)
}

func TestOnRetainedExpired(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	m := &storage.Message{
		ID:        retainedKey("a/b/c"),
		T:         storage.RetainedKey,
		TopicName: "a/b/c",
	}

	err = h.db.Save(m)
	require.NoError(t, err)

	r := new(storage.Message)
	err = h.db.One("ID", m.ID, r)
	require.NoError(t, err)
	require.Equal(t, m.TopicName, r.TopicName)

	h.OnRetainedExpired(m.TopicName)
	err = h.db.One("ID", m.ID, r)
	require.Error(t, err)
	require.Equal(t, storm.ErrNotFound, err)
}

func TestOnRetainedExpiredClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnRetainedExpired("a/b/c")
}

func TestOnRetainedExpiredNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnRetainedExpired("a/b/c")
}

func TestOnRetainMessageNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnRetainMessage(client, packets.Packet{}, 0)
}

func TestOnRetainMessageClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnRetainMessage(client, packets.Packet{}, 0)
}

func TestOnQosPublishThenQOSComplete(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Retain: true,
			Qos:    2,
		},
		Payload:   []byte("hello"),
		TopicName: "a/b/c",
	}

	h.OnQosPublish(client, pk, time.Now().Unix(), 0)

	r := new(storage.Message)
	err = h.db.One("ID", inflightKey(client, pk), r)
	require.NoError(t, err)
	require.Equal(t, pk.TopicName, r.TopicName)
	require.Equal(t, pk.Payload, r.Payload)

	// ensure dates are properly saved to bolt
	require.True(t, r.Sent > 0)
	require.True(t, time.Now().Unix()-2 < r.Sent)

	// OnQosDropped is a passthrough to OnQosComplete here
	h.OnQosDropped(client, pk)
	err = h.db.One("ID", inflightKey(client, pk), r)
	require.Error(t, err)
	require.Equal(t, storm.ErrNotFound, err)
}

func TestOnQosPublishNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnQosPublish(client, packets.Packet{}, time.Now().Unix(), 0)
}

func TestOnQosPublishClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnQosPublish(client, packets.Packet{}, time.Now().Unix(), 0)
}

func TestOnQosCompleteNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnQosComplete(client, packets.Packet{})
}

func TestOnQosCompleteClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnQosComplete(client, packets.Packet{})
}

func TestOnQosDroppedNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnQosDropped(client, packets.Packet{})
}

func TestOnSysInfoTick(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	info := &system.Info{
		Version:       "2.0.0",
		BytesReceived: 100,
	}

	h.OnSysInfoTick(info)

	r := new(storage.SystemInfo)
	err = h.db.One("ID", storage.SysInfoKey, r)
	require.NoError(t, err)
	require.Equal(t, info.Version, r.Version)
	require.Equal(t, info.BytesReceived, r.BytesReceived)
	require.NotSame(t, info, r)
}

func TestOnSysInfoTickNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	h.OnSysInfoTick(new(system.Info))
}

func TestOnSysInfoTickClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	h.OnSysInfoTick(new(system.Info))
}

func TestStoredClients(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with clients
	err = h.db.Save(&storage.Client{ID: "cl1", T: storage.ClientKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Client{ID: "cl2", T: storage.ClientKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Client{ID: "cl3", T: storage.ClientKey})
	require.NoError(t, err)

	r, err := h.StoredClients()
	require.NoError(t, err)
	require.Len(t, r, 3)
	require.Equal(t, "cl1", r[0].ID)
	require.Equal(t, "cl2", r[1].ID)
	require.Equal(t, "cl3", r[2].ID)
}

func TestStoredClientsNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredClients()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredClientsClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	v, err := h.StoredClients()
	require.Empty(t, v)
	require.Error(t, err)
}

func TestStoredSubscriptions(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with subscriptions
	err = h.db.Save(&storage.Subscription{ID: "sub1", T: storage.SubscriptionKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Subscription{ID: "sub2", T: storage.SubscriptionKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Subscription{ID: "sub3", T: storage.SubscriptionKey})
	require.NoError(t, err)

	r, err := h.StoredSubscriptions()
	require.NoError(t, err)
	require.Len(t, r, 3)
	require.Equal(t, "sub1", r[0].ID)
	require.Equal(t, "sub2", r[1].ID)
	require.Equal(t, "sub3", r[2].ID)
}

func TestStoredSubscriptionsNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredSubscriptions()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredSubscriptionsClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	v, err := h.StoredSubscriptions()
	require.Empty(t, v)
	require.Error(t, err)
}

func TestStoredRetainedMessages(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with messages
	err = h.db.Save(&storage.Message{ID: "m1", T: storage.RetainedKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Message{ID: "m2", T: storage.RetainedKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Message{ID: "m3", T: storage.RetainedKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Message{ID: "i3", T: storage.InflightKey})
	require.NoError(t, err)

	r, err := h.StoredRetainedMessages()
	require.NoError(t, err)
	require.Len(t, r, 3)
	require.Equal(t, "m1", r[0].ID)
	require.Equal(t, "m2", r[1].ID)
	require.Equal(t, "m3", r[2].ID)
}

func TestStoredRetainedMessagesNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredRetainedMessages()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredRetainedMessagesClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	v, err := h.StoredRetainedMessages()
	require.Empty(t, v)
	require.Error(t, err)
}

func TestStoredInflightMessages(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with messages
	err = h.db.Save(&storage.Message{ID: "i1", T: storage.InflightKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Message{ID: "i2", T: storage.InflightKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Message{ID: "i3", T: storage.InflightKey})
	require.NoError(t, err)

	err = h.db.Save(&storage.Message{ID: "m1", T: storage.RetainedKey})
	require.NoError(t, err)

	r, err := h.StoredInflightMessages()
	require.NoError(t, err)
	require.Len(t, r, 3)
	require.Equal(t, "i1", r[0].ID)
	require.Equal(t, "i2", r[1].ID)
	require.Equal(t, "i3", r[2].ID)
}

func TestStoredInflightMessagesNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredInflightMessages()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredInflightMessagesClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	v, err := h.StoredInflightMessages()
	require.Empty(t, v)
	require.Error(t, err)
}

func TestStoredInflightMessagesClientID(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// "a:b" checks the split is at the last ':'; hand-written rows come back with no client id
	h.OnQosPublish(&mqtt.Client{ID: "zen"}, packets.Packet{PacketID: 1, Origin: "mochi"}, time.Now().Unix(), 0)
	h.OnQosPublish(&mqtt.Client{ID: "a:b"}, packets.Packet{PacketID: 2, Origin: "mochi"}, time.Now().Unix(), 0)
	for _, id := range []string{"i1", storage.InflightKey + "_zen"} {
		err = h.db.Save(&storage.Message{ID: id, T: storage.InflightKey})
		require.NoError(t, err)
	}

	r, err := h.StoredInflightMessages()
	require.NoError(t, err)
	got := map[string]string{}
	for _, m := range r {
		got[m.ID] = m.ClientID
	}
	require.Equal(t, map[string]string{
		storage.InflightKey + "_zen:1": "zen",
		storage.InflightKey + "_a:b:2": "a:b",
		"i1":                           "",
		storage.InflightKey + "_zen":   "",
	}, got)
}

func TestStoredInflightMessagesClientIDNotStored(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	err = h.db.Save(&storage.Message{ID: "i1", T: storage.InflightKey, ClientID: "zen"})
	require.NoError(t, err)

	var m storage.Message
	err = h.db.One("ID", "i1", &m)
	require.NoError(t, err)
	require.Equal(t, "i1", m.ID)
	require.Empty(t, m.ClientID)
}

func TestStoredSysInfo(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	defer teardown(t, h.config.Path, h)

	// populate with sys info
	err = h.db.Save(&storage.SystemInfo{
		ID: storage.SysInfoKey,
		Info: system.Info{
			Version: "2.0.0",
		},
		T: storage.SysInfoKey,
	})
	require.NoError(t, err)

	r, err := h.StoredSysInfo()
	require.NoError(t, err)
	require.Equal(t, "2.0.0", r.Info.Version)
}

func TestStoredSysInfoNoDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	v, err := h.StoredSysInfo()
	require.Empty(t, v)
	require.NoError(t, err)
}

func TestStoredSysInfoClosedDB(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	err := h.Init(nil)
	require.NoError(t, err)
	teardown(t, h.config.Path, h)
	v, err := h.StoredSysInfo()
	require.Empty(t, v)
	require.Error(t, err)
}
