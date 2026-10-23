// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co, gsagula

package badger

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/hooks/storage"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
	"github.com/wind-c/comqtt/v2/mqtt/system"

	badgerhold "github.com/timshannon/badgerhold/v4"
)

const (
	// defaultDbFile is the default file path for the badger db file.
	defaultDbFile = ".badger"
)

// clientKey returns a primary key for a client.
func clientKey(cl *mqtt.Client) string {
	return cl.ID
}

// subscriptionKey returns a primary key for a subscription.
func subscriptionKey(cl *mqtt.Client, filter string) string {
	return subscriptionKeyByID(cl.ID, filter)
}

// subscriptionKeyByID returns a primary key for a subscription from a client id.
func subscriptionKeyByID(clientID, filter string) string {
	return storage.SubscriptionKey + "_" + clientID + ":" + filter
}

// retainedKey returns a primary key for a retained message.
func retainedKey(topic string) string {
	return storage.RetainedKey + "_" + topic
}

// inflightKeyPrefix returns the primary-key prefix shared by every inflight
// message stored for a given client id. Also matches ids that extend cid with
// ':', so filter with inflightClientID.
func inflightKeyPrefix(cid string) string {
	return storage.InflightKey + "_" + cid + ":"
}

// inflightKey returns a primary key for an inflight message.
func inflightKey(cl *mqtt.Client, pk packets.Packet) string {
	return inflightKeyPrefix(cl.ID) + pk.FormatID()
}

// inflightClientID returns the client id from an inflightKey key (everything before
// the last ':', since packet ids are numeric), or "" if the key is malformed.
func inflightClientID(key string) string {
	rest, ok := strings.CutPrefix(key, storage.InflightKey+"_")
	if !ok {
		return ""
	}
	i := strings.LastIndex(rest, ":")
	if i < 0 {
		return ""
	}
	return rest[:i]
}

// sysInfoKey returns a primary key for system info.
func sysInfoKey() string {
	return storage.SysInfoKey
}

// Options contains configuration settings for the BadgerDB instance.
type Options struct {
	Options *badgerhold.Options
	Path    string
}

// Hook is a persistent storage hook based using BadgerDB file store as a backend.
type Hook struct {
	mqtt.HookBase
	config *Options          // options for configuring the BadgerDB instance.
	db     *badgerhold.Store // the BadgerDB instance.
}

// ID returns the id of the hook.
func (h *Hook) ID() string {
	return "badger-db"
}

// Provides indicates which hook methods this hook provides.
func (h *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnSessionEstablish,
		mqtt.OnSessionEstablished,
		mqtt.OnDisconnect,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnRetainMessage,
		mqtt.OnWillSent,
		mqtt.OnQosPublish,
		mqtt.OnQosComplete,
		mqtt.OnQosDropped,
		mqtt.OnSysInfoTick,
		mqtt.OnClientExpired,
		mqtt.OnRetainedExpired,
		mqtt.StoredClients,
		mqtt.StoredInflightMessages,
		mqtt.StoredRetainedMessages,
		mqtt.StoredSubscriptions,
		mqtt.StoredSysInfo,
		mqtt.StoredClientByCid,
		mqtt.StoredSubscriptionsByCid,
		mqtt.StoredInflightMessagesByCid,
	}, []byte{b})
}

// Init initializes and connects to the badger instance.
func (h *Hook) Init(config any) error {
	if _, ok := config.(*Options); !ok && config != nil {
		return mqtt.ErrInvalidConfigType
	}

	if config == nil {
		config = new(Options)
	}

	h.config = config.(*Options)
	if h.config.Path == "" {
		h.config.Path = defaultDbFile
	}

	options := badgerhold.DefaultOptions
	options.Dir = h.config.Path
	options.ValueDir = h.config.Path
	options.Logger = h

	var err error
	h.db, err = badgerhold.Open(options)
	if err != nil {
		return err
	}

	return nil
}

// Stop closes the badger instance.
func (h *Hook) Stop() error {
	return h.db.Close()
}

// OnSessionEstablish deletes the previous session's inflight messages on a clean start [MQTT-3.1.2-4].
func (h *Hook) OnSessionEstablish(cl *mqtt.Client, pk packets.Packet) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	if pk.Connect.Clean {
		if err := h.deleteInflights(cl.ID); err != nil {
			h.Log.Error("failed to delete client inflight data", "error", err, "id", cl.ID)
		}
	}
}

// OnSessionEstablished adds a client to the store when their session is established.
func (h *Hook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	h.updateClient(cl)
}

// OnWillSent is called when a client sends a Will Message and the Will Message is removed from the client record.
func (h *Hook) OnWillSent(cl *mqtt.Client, pk packets.Packet) {
	h.updateClient(cl)
}

// updateClient writes the client data to the store.
func (h *Hook) updateClient(cl *mqtt.Client) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	props := cl.Properties.Props.Copy(false)
	in := &storage.Client{
		ID:              clientKey(cl),
		T:               storage.ClientKey,
		Remote:          cl.Net.Remote,
		Listener:        cl.Net.Listener,
		Username:        cl.Properties.Username,
		Clean:           cl.Properties.Clean,
		ProtocolVersion: cl.Properties.ProtocolVersion,
		Properties: storage.ClientProperties{
			SessionExpiryInterval: props.SessionExpiryInterval,
			AuthenticationMethod:  props.AuthenticationMethod,
			AuthenticationData:    props.AuthenticationData,
			RequestProblemInfo:    props.RequestProblemInfo,
			RequestResponseInfo:   props.RequestResponseInfo,
			ReceiveMaximum:        props.ReceiveMaximum,
			TopicAliasMaximum:     props.TopicAliasMaximum,
			User:                  props.User,
			MaximumPacketSize:     props.MaximumPacketSize,
		},
		Will: storage.ClientWill(cl.Properties.Will),
	}

	err := h.db.Upsert(in.ID, in)
	if err != nil {
		h.Log.Error("failed to upsert client data", "error", err, "data", in)
	}
}

// OnDisconnect removes a client from the store if their session has expired.
func (h *Hook) OnDisconnect(cl *mqtt.Client, _ error, expire bool) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	h.updateClient(cl)

	if !expire {
		return
	}

	if cl.StopCause() == packets.ErrSessionTakenOver {
		return
	}

	err := h.db.Delete(clientKey(cl), new(storage.Client))
	if err != nil {
		h.Log.Error("failed to delete client data", "error", err, "data", clientKey(cl))
	}
}

// OnSubscribed adds one or more client subscriptions to the store.
func (h *Hook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte, counts []int) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	var in *storage.Subscription
	for i := 0; i < len(pk.Filters); i++ {
		if reasonCodes[i] >= 0x80 { // refused filter; it was never subscribed or counted
			continue
		}
		in = &storage.Subscription{
			ID:     subscriptionKey(cl, pk.Filters[i].Filter),
			T:      storage.SubscriptionKey,
			Client: cl.ID,
			Filter: pk.Filters[i].Filter,
			Qos:    reasonCodes[i],
		}
		if pk.ProtocolVersion == 5 {
			in.Identifier = pk.Filters[i].Identifier
			in.NoLocal = pk.Filters[i].NoLocal
			in.RetainHandling = pk.Filters[i].RetainHandling
			in.RetainAsPublished = pk.Filters[i].RetainAsPublished
		}

		err := h.db.Upsert(in.ID, in)
		if err != nil {
			h.Log.Error("failed to upsert subscription data", "error", err, "data", in)
		}
	}
}

// OnUnsubscribed removes one or more client subscriptions from the store.
func (h *Hook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte, counts []int) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	for i := 0; i < len(pk.Filters); i++ {
		err := h.db.Delete(subscriptionKey(cl, pk.Filters[i].Filter), new(storage.Subscription))
		if err != nil {
			h.Log.Error("failed to delete subscription data", "error", err, "data", subscriptionKey(cl, pk.Filters[i].Filter))
		}
	}
}

// DeleteSubscription removes a client's subscription row outside the hook lifecycle
// (e.g. startup reconcile). Returns badgerhold.ErrNotFound if the row doesn't exist.
func (h *Hook) DeleteSubscription(clientID, filter string) error {
	if h.db == nil {
		return storage.ErrDBFileNotOpen
	}

	return h.db.Delete(subscriptionKeyByID(clientID, filter), new(storage.Subscription))
}

// DeleteSession removes the client row, subscription rows and inflight rows for
// a client id outside the hook lifecycle (e.g. the broker's session-expiry apply).
// It fires no hooks and touches no server state. Unlike DeleteSubscription it is
// idempotent: it returns nil if nothing is stored for the id.
func (h *Hook) DeleteSession(clientID string) error {
	if h.db == nil {
		return storage.ErrDBFileNotOpen
	}

	err := h.db.Delete(clientID, new(storage.Client))
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return err
	}

	subs, err := h.StoredSubscriptionsByCid(clientID)
	if err != nil {
		return err
	}
	// full scan of subscription rows per call (no index on Client); add one, or a
	// batched delete, if the discard volume at startup makes it matter.
	for _, sub := range subs {
		err = h.db.Delete(sub.ID, new(storage.Subscription))
		if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
			return err
		}
	}

	return h.deleteInflights(clientID)
}

// OnRetainMessage adds a retained message for a topic to the store.
func (h *Hook) OnRetainMessage(cl *mqtt.Client, pk packets.Packet, r int64) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	if r == -1 {
		err := h.db.Delete(retainedKey(pk.TopicName), new(storage.Message))
		if err != nil {
			h.Log.Error("failed to delete retained message data", "error", err, "data", retainedKey(pk.TopicName))
		}

		return
	}

	props := pk.Properties.Copy(false)
	in := &storage.Message{
		ID:          retainedKey(pk.TopicName),
		T:           storage.RetainedKey,
		FixedHeader: pk.FixedHeader,
		TopicName:   pk.TopicName,
		Payload:     pk.Payload,
		Created:     pk.Created,
		Origin:      pk.Origin,
		Properties: storage.MessageProperties{
			PayloadFormat:          props.PayloadFormat,
			MessageExpiryInterval:  props.MessageExpiryInterval,
			ContentType:            props.ContentType,
			ResponseTopic:          props.ResponseTopic,
			CorrelationData:        props.CorrelationData,
			SubscriptionIdentifier: props.SubscriptionIdentifier,
			TopicAlias:             props.TopicAlias,
			User:                   props.User,
		},
	}

	err := h.db.Upsert(in.ID, in)
	if err != nil {
		h.Log.Error("failed to upsert retained message data", "error", err, "data", in)
	}
}

// OnQosPublish adds or updates an inflight message in the store.
func (h *Hook) OnQosPublish(cl *mqtt.Client, pk packets.Packet, sent int64, resends int) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	props := pk.Properties.Copy(false)
	in := &storage.Message{
		ID:          inflightKey(cl, pk),
		T:           storage.InflightKey,
		Origin:      pk.Origin,
		PacketID:    pk.PacketID,
		FixedHeader: pk.FixedHeader,
		TopicName:   pk.TopicName,
		Payload:     pk.Payload,
		Sent:        sent,
		Created:     pk.Created,
		Properties: storage.MessageProperties{
			PayloadFormat:          props.PayloadFormat,
			MessageExpiryInterval:  props.MessageExpiryInterval,
			ContentType:            props.ContentType,
			ResponseTopic:          props.ResponseTopic,
			CorrelationData:        props.CorrelationData,
			SubscriptionIdentifier: props.SubscriptionIdentifier,
			TopicAlias:             props.TopicAlias,
			User:                   props.User,
		},
	}

	err := h.db.Upsert(in.ID, in)
	if err != nil {
		h.Log.Error("failed to upsert qos inflight data", "error", err, "data", in)
	}
}

// OnQosComplete removes a resolved inflight message from the store.
func (h *Hook) OnQosComplete(cl *mqtt.Client, pk packets.Packet) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err := h.db.Delete(inflightKey(cl, pk), new(storage.Message))
	if err != nil {
		h.Log.Debug("unable to delete inflight message data (benign, already deleted)", "error", err, "data", inflightKey(cl, pk))
	}
}

// OnQosDropped removes a dropped inflight message from the store.
func (h *Hook) OnQosDropped(cl *mqtt.Client, pk packets.Packet) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
	}

	h.OnQosComplete(cl, pk)
}

// OnSysInfoTick stores the latest system info in the store.
func (h *Hook) OnSysInfoTick(sys *system.Info) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	in := &storage.SystemInfo{
		ID:   sysInfoKey(),
		T:    storage.SysInfoKey,
		Info: *sys.Clone(),
	}

	err := h.db.Upsert(in.ID, in)
	if err != nil {
		h.Log.Error("failed to upsert $SYS data", "error", err, "data", in)
	}
}

// OnRetainedExpired deletes expired retained messages from the store.
func (h *Hook) OnRetainedExpired(filter string) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err := h.db.Delete(retainedKey(filter), new(storage.Message))
	if err != nil {
		h.Log.Error("failed to delete expired retained message data", "error", err, "id", retainedKey(filter))
	}
}

// deleteInflights deletes every stored inflight message for the client id and
// returns the first error.
func (h *Hook) deleteInflights(cid string) error {
	msgs, err := h.StoredInflightMessagesByCid(cid)
	if err != nil {
		return err
	}

	for _, m := range msgs {
		if err = h.db.Delete(m.ID, new(storage.Message)); err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
			return err
		}
	}

	return nil
}

// OnClientExpired deletes expired clients and their inflight messages from the store.
func (h *Hook) OnClientExpired(cl *mqtt.Client) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err := h.db.Delete(clientKey(cl), new(storage.Client))
	if err != nil {
		h.Log.Error("failed to delete expired client data", "error", err, "id", clientKey(cl))
	}

	if err := h.deleteInflights(cl.ID); err != nil {
		h.Log.Error("failed to delete client inflight data", "error", err, "id", cl.ID)
	}
}

// StoredClients returns all stored clients from the store.
func (h *Hook) StoredClients() (v []storage.Client, err error) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err = h.db.Find(&v, badgerhold.Where("T").Eq(storage.ClientKey))
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return
	}

	return v, nil
}

// StoredSubscriptions returns all stored subscriptions from the store.
func (h *Hook) StoredSubscriptions() (v []storage.Subscription, err error) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err = h.db.Find(&v, badgerhold.Where("T").Eq(storage.SubscriptionKey))
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return
	}

	return v, nil
}

// StoredRetainedMessages returns all stored retained messages from the store.
func (h *Hook) StoredRetainedMessages() (v []storage.Message, err error) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err = h.db.Find(&v, badgerhold.Where("T").Eq(storage.RetainedKey))
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return
	}

	return v, nil
}

// StoredInflightMessages returns all stored inflight messages from the store.
func (h *Hook) StoredInflightMessages() (v []storage.Message, err error) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err = h.db.Find(&v, badgerhold.Where("T").Eq(storage.InflightKey))
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return
	}

	for i := range v {
		v[i].ClientID = inflightClientID(v[i].ID)
	}

	return v, nil
}

// StoredClientByCid returns a single stored client from the store, if any.
func (h *Hook) StoredClientByCid(cid string) (v storage.Client, err error) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err = h.db.Get(cid, &v)
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return
	}

	return v, nil
}

// StoredSubscriptionsByCid returns all stored subscriptions for a single client id.
func (h *Hook) StoredSubscriptionsByCid(cid string) (v []storage.Subscription, err error) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err = h.db.Find(&v, badgerhold.Where("T").Eq(storage.SubscriptionKey).And("Client").Eq(cid))
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return
	}

	return v, nil
}

// StoredInflightMessagesByCid returns all stored inflight messages queued for a single client id.
func (h *Hook) StoredInflightMessagesByCid(cid string) (v []storage.Message, err error) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err = h.db.Find(&v, badgerhold.Where("T").Eq(storage.InflightKey).And("ID").HasPrefix(inflightKeyPrefix(cid)))
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return
	}

	// the prefix for "a" also matches "a:b"'s rows
	v = slices.DeleteFunc(v, func(m storage.Message) bool {
		return inflightClientID(m.ID) != cid
	})

	return v, nil
}

// StoredSysInfo returns the system info from the store.
func (h *Hook) StoredSysInfo() (v storage.SystemInfo, err error) {
	if h.db == nil {
		h.Log.Error("", "error", storage.ErrDBFileNotOpen)
		return
	}

	err = h.db.Get(storage.SysInfoKey, &v)
	if err != nil && !errors.Is(err, badgerhold.ErrNotFound) {
		return
	}

	return v, nil
}

// Errorf satisfies the badger interface for an error logger.
func (h *Hook) Errorf(m string, v ...interface{}) {
	h.Log.Error(fmt.Sprintf(strings.ToLower(strings.Trim(m, "\n")), v...), "v", v)

}

// Warningf satisfies the badger interface for a warning logger.
func (h *Hook) Warningf(m string, v ...interface{}) {
	h.Log.Warn(fmt.Sprintf(strings.ToLower(strings.Trim(m, "\n")), v...), "v", v)
}

// Infof satisfies the badger interface for an info logger.
func (h *Hook) Infof(m string, v ...interface{}) {
	h.Log.Info(fmt.Sprintf(strings.ToLower(strings.Trim(m, "\n")), v...), "v", v)
}

// Debugf satisfies the badger interface for a debug logger.
func (h *Hook) Debugf(m string, v ...interface{}) {
	h.Log.Debug(fmt.Sprintf(strings.ToLower(strings.Trim(m, "\n")), v...), "v", v)
}
