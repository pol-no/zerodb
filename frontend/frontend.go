package frontend

import "sync"

// Front ...
type Front struct {
	Wait *sync.WaitGroup
}

// Run ...
func (f *Front) Run() {
	f.Wait.Add(2)
	go httpService(f.Wait)
	go amqpService(f.Wait)
}
