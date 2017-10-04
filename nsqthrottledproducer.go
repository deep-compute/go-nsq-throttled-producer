package nsqthrottledproducer

import (
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/deep-compute/abool"
	"github.com/deep-compute/log"
	"github.com/deep-compute/stats"
	"github.com/nsqio/nsq/nsqd"
	"net/http"
	"sync"
	"time"
)

type TopicFullError struct {
	Topic string
}

func (t *TopicFullError) Error() string {
	return fmt.Sprintf("Topic %s is full, please call WaitForTopic before trying again", t.Topic)
}

// DefaultProducerThreshold is assigned to NsqThrottledProducer DefaultThreshold
// when no value is provided.
const DefaultProducerThreshold int64 = 1000
const DefaultProducerStatusInterval time.Duration = 1 * time.Second

// NsqThrottledProducer is a producer that checks the nsq depth before publishing.
// If the depth is greater than the specified threshold for that topic, it
// does not publish.
type NsqThrottledProducer struct {
	NSQDHost              string
	NSQDTCPPort           int
	NSQDHTTPPort          int
	DefaultThreshold      int64
	DefaultStatusInterval time.Duration

	producer *nsq.Producer
	config   *nsq.Config

	topicThreshold map[string]int64
	topicFreeBool  map[string]*abool.AtomicBool
	topicFreeCond  map[string]*sync.Cond
	topicFullError map[string]*TopicFullError
	topicToggledAt map[string]time.Time
}

func (p *NsqThrottledProducer) Init(topicThresholdMap map[string]int64) error {
	if p.DefaultThreshold < 0 {
		return fmt.Errorf("Default Threshold cannot be negative")
	}

	if p.DefaultThreshold == 0 {
		p.DefaultThreshold = DefaultProducerThreshold
	}

	if p.DefaultStatusInterval == 0 {
		p.DefaultStatusInterval = DefaultProducerStatusInterval
	}

	config := nsq.NewConfig()
	loc := fmt.Sprintf("%s:%d", p.NSQDHost, p.NSQDTCPPort)
	producer, err := nsq.NewProducer(loc, config)
	if err != nil {
		return err
	}

	p.producer = producer
	if err := producer.Ping(); err != nil {
		return err
	}

	p.topicThreshold = make(map[string]int64)
	p.topicFreeCond = make(map[string]*sync.Cond)
	p.topicFreeBool = make(map[string]*abool.AtomicBool)
	p.topicFullError = make(map[string]*TopicFullError)
	p.topicToggledAt = make(map[string]time.Time)

	for topic, threshold := range topicThresholdMap {
		if err := p.addTopic(topic, threshold); err != nil {
			return err
		}
	}

	go p.checkNSQDStatus()
	return nil
}

// addTopic adds a topic to the NsqThrottledProducer with the given threshold
func (p *NsqThrottledProducer) addTopic(topic string, threshold int64) error {
	if threshold < 0 {
		return fmt.Errorf("Threshold cannot be negative")
	}
	if threshold == 0 {
		threshold = p.DefaultThreshold
	}

	p.topicThreshold[topic] = threshold
	p.topicFreeCond[topic] = &sync.Cond{L: &sync.Mutex{}}
	p.topicFreeBool[topic] = abool.New()
	p.topicFreeBool[topic].SetTo(false)
	p.topicToggledAt[topic] = time.Now()
	p.topicFullError[topic] = &TopicFullError{Topic: topic}
	return nil
}

// checkNSQDStatus checks the status of the given NSQD to find the depth
// of each known topic.
func (p *NsqThrottledProducer) checkNSQDStatus() {
	statusURL := fmt.Sprintf("http://%s:%d/stats?format=json", p.NSQDHost, p.NSQDHTTPPort)
	log.Info("monitoring nsqd status", "url", statusURL, "interval", p.DefaultStatusInterval)

	//TODO: re-check the structure code in all aspects.
	nsqstats := struct {
		Health string            `json:"health"`
		Topics []nsqd.TopicStats `json:"topics"`
	}{}

	dummyChannels := make([]nsqd.ChannelStats, 0)
	var dummyStruct struct{}
	seenTopics := make(map[string]struct{})

	for {
		resp, err := http.Get(statusURL)
		if err != nil {
			panic(err)
		}

		// If only they allowed to reinit so we could change the source
		// we would have less garbage :/
		decoder := json.NewDecoder(resp.Body)
		if err := decoder.Decode(&nsqstats); err != nil {
			panic(err)
		}
		if nsqstats.Health != "OK" {
			panic(fmt.Errorf("bad statuscode"))
		}

		for _, topicStat := range nsqstats.Topics {
			seenTopics[topicStat.TopicName] = dummyStruct
		}
		// any known topics that arent seen need to be part of
		// these calculations
		for topic, _ := range p.topicThreshold {
			if _, exists := seenTopics[topic]; exists {
				continue
			}
			dummyTopic := nsqd.TopicStats{
				TopicName: topic,
				Channels:  dummyChannels,
				Depth:     0,
			}
			nsqstats.Topics = append(nsqstats.Topics, dummyTopic)
			seenTopics[topic] = dummyStruct
		}

		for _, topicStat := range nsqstats.Topics {
			topic := topicStat.TopicName
			if _, exists := p.topicThreshold[topic]; !exists {
				continue
			}
			delete(seenTopics, topic)
			// We are concerned with overall depth - all channels
			// TODO do we have to take care of BackendDepth ?
			topicDepth := topicStat.Depth
			for _, chanStat := range topicStat.Channels {
				topicDepth += chanStat.Depth
			}
			topicStat.Depth = topicDepth

			topicFreeBool := p.topicFreeBool[topic]
			topicFreeCond := p.topicFreeCond[topic]
			if topicStat.Depth >= p.topicThreshold[topic] {
				// if it is already disabled, continue
				if !topicFreeBool.IsSet() {
					continue
				}

				log.Debug("disabling publishing on topic",
					"topic", topic,
					"depth", topicStat.Depth,
					"threshold", p.topicThreshold[topic],
				)
				topicFreeBool.UnSet()
				// time that this topic spent being free.
				stats.TimingSince(fmt.Sprintf("%s.enabled", topic), p.topicToggledAt[topic])
				p.topicToggledAt[topic] = time.Now()
			} else {
				// if it is already free to take more, continue
				if topicFreeBool.IsSet() {
					continue
				}

				log.Debug("re-enabling publishing on topic",
					"topic", topic,
					"depth", topicStat.Depth,
					"threshold", p.topicThreshold[topic],
				)
				topicFreeBool.Set()
				topicFreeCond.Broadcast()

				// time that this topic spent being disabled.
				stats.TimingSince(fmt.Sprintf("%s.disabled", topic), p.topicToggledAt[topic])
				p.topicToggledAt[topic] = time.Now()
			}
		}

		time.Sleep(p.DefaultStatusInterval)
	}
}

// WaitForTopic blocks until the topic is free to accept more.
// If the topic was already free, this returns immediately
func (p *NsqThrottledProducer) WaitForTopic(topic string) {
	// TODO stats to wait
	topicFreeCond := p.topicFreeCond[topic]
	topicFreeBool := p.topicFreeBool[topic]
	topicFreeCond.L.Lock()
	for {
		if topicFreeBool.IsSet() {
			break
		}

		log.Debug("waiting for topic", "topic", topic)
		topicFreeCond.Wait()
		log.Debug("done waiting for topic", "topic", topic)
	}
	topicFreeCond.L.Unlock()
}

// Publish publishes data on a particular topic.
// It will block and wait till the threshold is cleared.
//  err := p.Publish("topic", data)
//  if err != nil {
//   if e, ok := err.(*TopicFullError); ok {
//     p.WaitForTopic("topic");
//   }
//  }
func (p *NsqThrottledProducer) Publish(topic string, body []byte) error {
	stats.Incr(fmt.Sprintf("%s.publish", topic), 1)
	if p.topicFreeBool[topic].IsSet() {
		return p.producer.Publish(topic, body)
	}

	stats.Incr(fmt.Sprintf("%s.publish.full", topic), 1)
	// cannot publish on topic
	return p.topicFullError[topic]
}

// MultiPublish is the same as publish except for multiple messages
// to the same topic.
func (p *NsqThrottledProducer) MultiPublish(topic string, bodies [][]byte) error {
	stats.Incr(fmt.Sprintf("%s.multipublish", topic), 1)
	if p.topicFreeBool[topic].IsSet() {
		return p.producer.MultiPublish(topic, bodies)
	}

	stats.Incr(fmt.Sprintf("%s.multipublish.full", topic), 1)
	// cannot publish on topic
	return p.topicFullError[topic]
}
