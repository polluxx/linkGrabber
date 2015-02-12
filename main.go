package main

import (
	"github.com/polluxx/linkGrabber/worker"
	"time"
	"log"
	"net/http"
)

func main() {
	updated := make(chan bool)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			r.ParseForm();
			queryParams := make(map[string]string)
			for index, value := range r.Form {
				queryParams[index] = value[0];
			}

			if(queryParams["pwd"] != "" && queryParams["pwd"] == "A9Dlg7" && queryParams["update"] != "") {
				updated <- true
			}
			log.Printf("%v", queryParams)

	})



	s := &http.Server{
			Addr:           ":8681",
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}

	log.Fatal(s.ListenAndServe())


	go func() {
		for {
			select {
			case <-time.After(1 * 24 * time.Hour):
				log.Printf("updated timely")
				_ = worker.Search()
			case <-updated:
				log.Printf("updated manually")
				_ = worker.Search()
			}
		}
	}()


}
