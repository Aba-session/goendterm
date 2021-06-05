package main

import (
  "bytes"
  "fmt"
  "io/ioutil"
  "sort"
  "sync"
  "time"
)

type record struct {
  word    []byte
  counter int
}

type job func(in, out chan interface{})

func ExecutePipeline(jobs ...job) {
  wg := &sync.WaitGroup{}

  in := make(chan interface{}, 100)
  out := make(chan interface{}, 100)

  for _, job := range jobs {
    wg.Add(1)
    go worker(wg, job, in, out);
    in = out
    out = make(chan interface{}, 100)
  }

  wg.Wait()
}

func worker(wg *sync.WaitGroup, j job, in, out chan interface{}) {
  defer wg.Done()
  defer close(out)
  j(in, out)
}

func reader(out chan interface{}, file []byte) {
  var word []byte

  for _, c := range file {
    if c >= 65 && c <= 90 {
      word = append(word, c + 32)
    } else if c >= 97 && c <= 122 {
      word = append(word, c )
    } else if len(word) > 0 {
      out <- word
      word = []byte{}
    }
  }
}

func counter(in, out chan interface{}){
  var records []record

  for word := range in {
    found := false

    for i, r := range records {
      // slowest part
      if bytes.Equal(r.word, word.([]byte)) {
        records[i].counter++
        found = true
        break
      }
    }

    if !found {
      records = append(records, record{ word : word.([]byte), counter : 1 })
    }
  }

  sort.Slice(records, func(a, b int) bool {
    return records[a].counter > records[b].counter
  })

  for i := 0; i < 20; i++ {
    out <- records[i]
  }
}

func writer(in chan interface{}) {
  for r := range in {
    fmt.Printf("  %d %s\n", r.(record).counter, r.(record).word)
  }
}

func main() {
  start := time.Now()

  // load content of mobydick to memory
  file, _ := ioutil.ReadFile("mobydick.txt")

  jobs := []job {
    // reader
    func(in, out chan interface{}) {
      reader(out, file)
    },

    // counter
    func(in, out chan interface{}) {
      counter(in, out)
    },

    // writer
    func(in, out chan interface{}) {
      writer(in)
    },
  }

  ExecutePipeline(jobs...)

  fmt.Printf("Process took %s\n", time.Since(start))
}
