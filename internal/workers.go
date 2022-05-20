package internal

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type WorkResult struct {
	Started  time.Time
	Finished time.Time
	Result   any
	Err      error
}

type Work func() (any, error)

type Worker struct {
	id  int
	log *log.Logger
	in  <-chan Work
	out chan<- WorkResult
}

func NewWorker(id int, in <-chan Work, out chan<- WorkResult) Worker {
	return Worker{
		id:  id,
		log: log.New(os.Stdout, fmt.Sprintf("[worker %d] ", id), log.LstdFlags),
		in:  in,
		out: out,
	}
}

func (w *Worker) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	for work := range w.in {
		zult := WorkResult{
			Started: time.Now().UTC(),
		}
		val, err := work()
		zult.Finished = time.Now().UTC()
		zult.Result = val
		zult.Err = err
		w.out <- zult
	}
	w.log.Printf("done id=%v", w.id)
}
