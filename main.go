package aekazoe

import (
	"errors"
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

// ErrEmptyQueueName returns when queue name is empty
var ErrEmptyQueueName = errors.New("QueueName is empty")

// Kazoe means 数え. Counter in japanese.
type Kazoe interface {
	PurgeAsyncCount(c context.Context) error
	IncrementAsyncByString(c context.Context, id string) error
	IncrementAsyncByInt(c context.Context, id int) error
	IncrementAsyncByInt64(c context.Context, id int64) error
	IncrementAsyncByKey(c context.Context, key *datastore.Key) error
	DecrementAsyncByString(c context.Context, id string) error
	DecrementAsyncByInt(c context.Context, id int) error
	DecrementAsyncByInt64(c context.Context, id int64) error
	DecrementAsyncByKey(c context.Context, key *datastore.Key) error
	CollectDeltaByString(c context.Context) (map[string]int, func() error, error)
	CollectDeltaByInt(c context.Context) (map[int]int, func() error, error)
	CollectDeltaByInt64(c context.Context) (map[int64]int, func() error, error)
	CollectDeltaByKey(c context.Context) (map[*datastore.Key]int, func() error, error)
}

type kazoeImpl struct {
	QueueName string
}

// New kazoe interface returns.
func New(queueName string) (Kazoe, error) {
	if queueName == "" {
		return nil, ErrEmptyQueueName
	}

	return &kazoeImpl{QueueName: queueName}, nil
}

func (kazoe *kazoeImpl) PurgeAsyncCount(c context.Context) error {
	if kazoe.QueueName == "" {
		return ErrEmptyQueueName
	}

	err := taskqueue.Purge(c, kazoe.QueueName)
	if err != nil {
		return err
	}

	return nil
}

func (kazoe *kazoeImpl) incrementAsync(c context.Context, id []byte) error {
	if kazoe.QueueName == "" {
		return ErrEmptyQueueName
	}

	t := &taskqueue.Task{
		Payload: id,
		Tag:     "++",
		Method:  "PULL",
	}

	log.Debugf(c, "inc id: %s, queueName: %s", string(id), kazoe.QueueName)
	_, err := taskqueue.Add(c, t, kazoe.QueueName)
	if err != nil {
		return err
	}

	return nil
}

func (kazoe *kazoeImpl) IncrementAsyncByString(c context.Context, id string) error {
	return kazoe.incrementAsync(c, []byte(id))
}

func (kazoe *kazoeImpl) IncrementAsyncByInt(c context.Context, id int) error {
	return kazoe.incrementAsync(c, []byte(strconv.Itoa(id)))
}

func (kazoe *kazoeImpl) IncrementAsyncByInt64(c context.Context, id int64) error {
	return kazoe.incrementAsync(c, []byte(strconv.FormatInt(id, 10)))
}

func (kazoe *kazoeImpl) IncrementAsyncByKey(c context.Context, key *datastore.Key) error {
	return kazoe.incrementAsync(c, []byte(key.Encode()))
}

func (kazoe *kazoeImpl) decrementAsync(c context.Context, id []byte) error {
	if kazoe.QueueName == "" {
		return ErrEmptyQueueName
	}

	t := &taskqueue.Task{
		Payload: id,
		Tag:     "--",
		Method:  "PULL",
	}

	log.Debugf(c, "dec id: %s, queueName: %s", string(id), kazoe.QueueName)
	_, err := taskqueue.Add(c, t, kazoe.QueueName)
	if err != nil {
		return err
	}

	return nil
}

func (kazoe *kazoeImpl) DecrementAsyncByString(c context.Context, id string) error {
	return kazoe.decrementAsync(c, []byte(id))
}

func (kazoe *kazoeImpl) DecrementAsyncByInt(c context.Context, id int) error {
	return kazoe.decrementAsync(c, []byte(strconv.Itoa(id)))
}

func (kazoe *kazoeImpl) DecrementAsyncByInt64(c context.Context, id int64) error {
	return kazoe.decrementAsync(c, []byte(strconv.FormatInt(id, 10)))
}

func (kazoe *kazoeImpl) DecrementAsyncByKey(c context.Context, key *datastore.Key) error {
	return kazoe.decrementAsync(c, []byte(key.Encode()))
}

func (kazoe *kazoeImpl) CollectDeltaByString(c context.Context) (map[string]int, func() error, error) {
	if kazoe.QueueName == "" {
		return nil, nil, ErrEmptyQueueName
	}

	tasks, err := taskqueue.Lease(c, 1000, kazoe.QueueName, 10*60)
	if err != nil {
		return nil, nil, err
	}

	deltaMap := map[string]int{}
	for _, task := range tasks {
		tagID := string(task.Payload)
		delta := 0
		switch task.Tag {
		case "++":
			delta = 1
		case "--":
			delta = -1
		default:
			log.Warningf(c, "unknown tag %s in %s", task.Tag, tagID)
			continue
		}
		if v, ok := deltaMap[tagID]; ok {
			v += delta
			deltaMap[tagID] = v
		} else {
			deltaMap[tagID] = delta
		}
	}

	closer := func() error {
		if kazoe.QueueName == "" {
			return ErrEmptyQueueName
		}

		err = taskqueue.DeleteMulti(c, tasks, kazoe.QueueName)
		if err != nil {
			return err
		}
		return nil
	}

	return deltaMap, closer, nil
}

func (kazoe *kazoeImpl) CollectDeltaByInt(c context.Context) (map[int]int, func() error, error) {
	deltaMap, closer, err := kazoe.CollectDeltaByString(c)
	if err != nil {
		return nil, nil, err
	}

	newDeltaMap := make(map[int]int, len(deltaMap))
	for k, v := range deltaMap {
		intK, err := strconv.Atoi(k)
		if err != nil {
			return nil, nil, err
		}
		newDeltaMap[intK] = v
	}

	return newDeltaMap, closer, nil
}

func (kazoe *kazoeImpl) CollectDeltaByInt64(c context.Context) (map[int64]int, func() error, error) {
	deltaMap, closer, err := kazoe.CollectDeltaByString(c)
	if err != nil {
		return nil, nil, err
	}

	newDeltaMap := make(map[int64]int, len(deltaMap))
	for k, v := range deltaMap {
		int64K, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, nil, err
		}
		newDeltaMap[int64K] = v
	}

	return newDeltaMap, closer, nil
}

func (kazoe *kazoeImpl) CollectDeltaByKey(c context.Context) (map[*datastore.Key]int, func() error, error) {
	deltaMap, closer, err := kazoe.CollectDeltaByString(c)
	if err != nil {
		return nil, nil, err
	}

	newDeltaMap := make(map[*datastore.Key]int, len(deltaMap))
	for k, v := range deltaMap {
		key, err := datastore.DecodeKey(k)
		if err != nil {
			return nil, nil, err
		}
		newDeltaMap[key] = v
	}

	return newDeltaMap, closer, nil
}
