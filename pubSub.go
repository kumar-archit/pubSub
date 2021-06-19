package main

import (
	"fmt"
	"strings"
	"sync"
	// "time"
)
var wg sync.WaitGroup //1

//Go 
// map key is topicID and value is slice of multiple subscriptionID
var topics  = make(map[string][]string)
var retryCountsiz = 1
type subscription struct {
	isSubscribed bool
	subscriberName string
	topicID string
}

type message struct{
	messageID string
	messageStr string
}

var subscriptions = make(map[string]subscription)

var channelsForTopics = make(map[string]chan message)
var MapForAcks = make(map[string] chan bool)

type SFunc func(string)

func CreateTopic ( topicID string){
	var topic []string
	for key, value := range topics {
		if(strings.TrimRight(topicID, "\n") == key ){
			topic = value
			fmt.Printf("topicID already exists with following subscriptions: %v\n", topic)
		}
	}
	topics[topicID] = topic
	fmt.Printf("topicID : %v created \n", topicID)		
}

func AddSubscription(topicID,SubscriptionID string){
	var topic []string
	for key, value := range topics {
		if(strings.TrimRight(topicID, "\n") == key ){
			topic = append(value, SubscriptionID)
			fmt.Printf("Added %v, subscriptions for topic %v : %v\n", SubscriptionID, topicID, topic)
		}
	}
	if(len(topic) == 0){
		fmt.Printf("topicID : %v not found \n", topicID)		
	}else{
		topics[topicID] = topic
	}
}

func DeleteSubscription(SubscriptionID string){
	var topic []string
	for key, value := range topics {	
		siz := len(value)
		for i:=0; i<siz; i++	{
			if(value[i] == SubscriptionID){
				if(i+1 < len(value)){
					topic = append(value[:i], value[i+1])
				} else {
					topic = append(value[:i])
				}
				i = len(value)
				topics[key] = topic
				fmt.Printf("Removed subscriptionID : %v  for topic %v, remaining subscriptionID for topic: %v\n", key, SubscriptionID, topic)
			}
		}
	}
	delete(subscriptions, SubscriptionID)
}
//Called by Subscriber to intimate the Subscription that the message has been received and processed
func Ack(SubscriptionID, MessageID string){
	defer wg.Done()
	go func(){
		MapForAcks[MessageID] <- true	 
	}()

}

func printMessage(SubscriptionID string){
	defer wg.Done() //3
	msg := <-channelsForTopics[subscriptions[SubscriptionID].topicID]
	fmt.Printf("Got a message for subscriber: %v\n",  subscriptions[SubscriptionID].subscriberName)

	fmt.Printf("messageID: %v\n", msg.messageID)
	fmt.Printf("messageStr: %v\n", msg.messageStr)
	Ack(SubscriptionID, msg.messageID)
}

//SubscriberFunc is the subscriber which is executed for each message of subscription.
func Subscribe(SubscriptionID string, SubscriberFunc SFunc) {
	go SubscriberFunc(SubscriptionID)
}
func shallRetry(messageID string, lwg *sync.WaitGroup) bool{
	defer lwg.Done()
	ackMessage := false
	ackMessage= <- MapForAcks[messageID]
	return ackMessage
}

func Publish(topicID string, messageToPublish message){
	fmt.Printf("In Publish():: Start %v\n", topicID)
	for key, value := range topics {
		if(key == topicID){
			siz := len(value)
			for i:=0; i<siz;i++ {
				subscriberID := value[i]
				var ackMessage = false
				for retryCount:=0; retryCount<retryCountsiz &&!ackMessage; retryCount++	{
					wg.Add(2) // 2
					go func(){
						channelsForTopics[topicID] <- messageToPublish
						var lwg sync.WaitGroup
						lwg.Add(1)
						Subscribe(subscriberID, printMessage)
						ackMessageChan := make(chan bool)
						MapForAcks[messageToPublish.messageID] = ackMessageChan
						ackMessage =shallRetry(messageToPublish.messageID, &lwg)
						lwg.Wait()
					}()
				}
			}
		}
	}
}

// func DeleteTopic(TopicID string){
//Should DeleteTopic also call DeleteSubscription ?
// }


func UnSubscribe(SubscriptionID string){
	subsNew  :=  subscription{false, subscriptions[SubscriptionID].subscriberName, subscriptions[SubscriptionID].topicID}
	subscriptions[SubscriptionID] = subsNew
	fmt.Printf("UnSubscribed for SubscriptionID: %v\n", SubscriptionID)
}

func main() {
	fmt.Println("Main start: ")	
	CreateTopic("1")
	CreateTopic("2")
	CreateTopic("3")
	// fmt.Println(time.Now().Sub(start))
	subs1  :=  subscription{true, "name1", "1"}
	subs2  :=  subscription{true, "name2", "1"}
	subs3  :=  subscription{true, "name3", "2"}
	subs4  :=  subscription{true, "name4", "3"}
	subscriptions["101"] = subs1
	subscriptions["102"] = subs2
	subscriptions["201"] = subs3
	subscriptions["301"] = subs4
	AddSubscription("1","101")
	AddSubscription("1","102")
	AddSubscription("2","201")
	AddSubscription("3","301")
	fmt.Printf("subscriptions for topics : %v \n", topics);
	DeleteSubscription("301")
	fmt.Printf("subscriptions: %v\n", subscriptions);
	UnSubscribe("102")
	fmt.Printf("subscriptions: %v\n", subscriptions);
	chan1 := make(chan message, 2)
	channelsForTopics["1"] = chan1
	chan2 := make(chan message, 1)
	channelsForTopics["2"] = chan2
	msg1 := message{"11111", "hello there"}
	msg2 := message{"12222", "How are you!!"}
	Publish("1", msg1)
	Publish("2", msg2)

	wg.Wait() // 4

// channel for each topicID as a map
//fan out channels for all the subscriberID for each topic
//for each fan out channel have a timeout in go func which upon timeout sends value again
//in each of these fan out channel have a return channel which will send an ack 
// when ack is received do not wait for ack and do not retry 
}