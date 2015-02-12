package worker

import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "log"
import "fmt"
/*
type Conn struct {
	Connect *mysql.Connection
}
*/
var (
	Host string = "10.1.18.111"
	Port int = 8060
	User string = "master"
	Pass string = "gtnhjdbx"
	Dbname string = "coob"
)

func DataBind() {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@%s/%s", User, Pass, Host, Dbname))
	if (err != nil) {
		log.Print("Cannot establish connection to MySql server")
	}
	defer db.Close()

	err = db.Ping()
	if (err != nil) {
		log.Print("MySql server has gone away")
	}

	//var name string
	stmtOut, err := db.Prepare("SELECT name FROM rewrites limit 1,10")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtOut.Close()
}
