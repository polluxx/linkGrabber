package worker

import (
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"log"
	"github.com/gocql/gocql"
	"strconv"
	"time"

)

const Url string = "http://prodvigator.ua/api/v2/url_keywords?query=http://%s&token=%s&page=%d&page_size=%d"

var (
	Token string = "990f3b5aadb8bcfe54f7dd013001ce81"
	ProjectStr string = "http://dom.ria.com"
)

type Result struct {
	Keyword string
	Position string
	Dynamic string
	Cost string
	Region_queries_count string
	Search_concurrency string
	Concurrency string
	Url_id string
	Date string
	Right_spell_id string
	Id string
	Geo_names string
	Types string
	Url string
	Domain string
}

type Response struct {
	Result []Result
	Status_msg string
	Status_code int
	Queries_left int
}

type Resp struct {
	Code int
	Message string
	Data []Result
}

var (
	Servers = []string{"10.1.51.65","10.1.51.66"}
	//Servers = []string{"10.1.18.122"}
	Keyspace string = "avp"
	Table string = "training_set"
	Limit int = 200
	Projects = map[int]string{1:"auto.ria.com",2:"ria.com",3:"dom.ria.com",5:"market.ria.com",}

)

var ProjectsFullList = map[string]int {
	"auto.ria.com": 1,

	"ria.com": 2,

	"dom.ria.com": 3,

	"market.ria.com": 5,

	"zapchasti.ria.com": 2,
	"vinnica.ria.com": 2,
	"dnepropetrovsk.ria.com": 2,
	"doneck.ria.com": 2,
	"zhitomir.ria.com": 2,
	"zaporozhye.ria.com": 2,
	"ivano-frankovsk.ria.com": 2,
	"kyiv.ria.com": 2,
	"kirovograd.ria.com": 2,
	"lugansk.ria.com": 2,
	"lutsk.ria.com": 2,
	"lviv.ria.com": 2,
	"nikolaev.ria.com": 2,
	"odessa.ria.com": 2,
	"poltava.ria.com": 2,
	"rovno.ria.com": 2,
	"simferopol.ria.com": 2,
	"sumy.ria.com": 2,
	"ternopol.ria.com": 2,
	"uzhgorod.ria.com": 2,
	"kharkov.ria.com": 2,
	"herson.ria.com": 2,
	"khmelnitsky.ria.com": 2,
	"cherkassy.ria.com": 2,
	"chernigov.ria.com": 2,
	"chernovtsy.ria.com": 2,
}

func Search() []string {
	ch := make(chan string)
	//channels := make(map[int] chan string)
	var responses []string

	//DataBind()


		for projectName, id := range ProjectsFullList {
			//log.Print(projectName, id)

			go func() {
				ch <- Grabber(id, projectName)
			}()

			log.Print(projectName)
		}

	for {

		result := <- ch
		responses = append(responses, result)
	}

	return responses
}

//

// ERROR
type ResponseError struct {
	When time.Time
	What string
}

func (e ResponseError) Error() string {
	return fmt.Sprintf("%v: %v", e.When, e.What)
}

func Grabber(project int, projectName string) string{

	var (
		err error
	)

	// #toDo : use project

	out := make(chan Resp)
	quit := make(chan bool)
	var response string

	counter := 1
	counterPages := 1

	Grab(projectName, 1, out)
	//reupload := time.After(60 * time.Second)

	for {
		select {
		case result := <-out:

			if (len(result.Data) == 0) {
				log.Print("service finished ", counterPages, projectName, result)
				quit <- true
			}

			counter++
			counterPages++

			log.Print(result.Data[0], len(result.Data), counterPages, counter)
			//data = append(data, result.Data...)
			Put(result)
			if (counter == 6) {

				counter = 1
				Grab(projectName, counterPages, out)
				//return err
			}
		case <-quit:
			response = err.Error()
			return response

			/*case <-time.After(180 * time.Second):
			Grab(project, 1, out)
		}*/
		}
	}

	response = "OK"
	return response
}

func Grab(projectName string, pageS int, out chan Resp) {

	log.Print(pageS)

	reupload := time.After(2 * time.Second)

	select {
	case <- reupload:
		for page := pageS; page < pageS+5; page++{

			//go func(page int) {
			go Parse(projectName, page, out)
			//}(page)

		}
	}

}

func Parse(projectName string, page int, out chan Resp) {
	var response = Resp{}

	resp, err := http.Get(fmt.Sprintf(Url, projectName, Token, page, Limit))
	if (err != nil) {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		out <- response

		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Print(err)
		response.Code = 500
		response.Message = err.Error()
		//return response
		out <- response

		return
	}

	var jsn Response
	err = json.Unmarshal(body, &jsn)
	if err != nil {
		log.Print(err)
		response.Code = 500
		response.Message = err.Error()
		//return response
		out <- response

		return
	}

	if(jsn.Status_code != 200) {
		response.Code = jsn.Status_code
		response.Message = jsn.Status_msg
		//return response
		out <- response

		return
	}

	response.Code = 200
	response.Data = jsn.Result

	//log.Print(jsn.Result)
	out <- response

	//return response
}

func Put(data Resp) error{
	if (data.Code != 200) {
		return ResponseError{
			time.Now(),
			data.Message,
		}
	}

	cluster := gocql.NewCluster(Servers[0], Servers[1])
	cluster.Keyspace = Keyspace
	//cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()
	if (err != nil) {
		return err
	}
	defer session.Close()

	var (
		position int64
		dynamic int64
		cost float64
		region_queries int64
		search_concurrency int64
		concurrency int64
		url_id int64
		datetime time.Time
		right_spell_id int64
		id int64
	)


	location, _ := time.LoadLocation("Europe/Kiev")

	for _, row := range data.Data {



		position, err = strconv.ParseInt(row.Position, 10, 64)
		dynamic, err = strconv.ParseInt(row.Dynamic, 10, 64)
		//cost, err = strconv.ParseFloat(row.Cost, 64)
		region_queries, err = strconv.ParseInt(row.Region_queries_count, 10, 64)
		search_concurrency, err = strconv.ParseInt(row.Search_concurrency, 10, 64)
		concurrency, err = strconv.ParseInt(row.Concurrency, 10, 64)
		//url_id, err = strconv.ParseInt(row.Url_id, 10, 64)
		//right_spell_id, err = strconv.ParseInt(row.Right_spell_id, 10, 64)
		//id, err = strconv.ParseInt(row.Id, 10, 64)

		//fromT := fmt.Sprintf("%sT00:00:00Z", row.Date)
		//datetime, err = time.ParseInLocation(time.RFC3339, fromT, location)
		
		if(err != nil) {
			return err
		}



		if(dynamic == 0) {
			dynamic = 0
		}
		
		if err = session.Query(`INSERT INTO training_set
							(keyword, position, dynamic,
 							region_queries_count, search_concurrency,
 							concurrency, geo_names, url, domain)
							VALUES
 							(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			row.Keyword, position, dynamic,
			region_queries, search_concurrency,
			concurrency, row.Geo_names,
			row.Url, row.Domain).Exec(); err != nil {
				return err
		}

		//log.Printf("%v", row)
	}


	return err
}
