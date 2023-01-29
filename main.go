package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Flags struct {
	threads int    // concurrency
	runtime int    // seconds
	dsn     string // MySQL DSN
	runtype string // run type
}

var QUERY_COUNT int64
var MU sync.Mutex
var flags Flags

func init() {
	flags.threads = *flag.Int("thread", 10, "number of concurrent threads")
	flags.runtime = *flag.Int("runtime", 600, "test time")
	flag.StringVar(&flags.dsn, "dsn", "test:test@tcp(127.0.0.1:4000)/test", "mysql dsn")
	flag.StringVar(&flags.runtype, "runtype", "init", "run type")
}

func createTable() error {
	createDBSQL := "create database if not exist autoinc"
	createTableSQL := "create table if not exist autoinc.autoinctest (id int primary key auto_increment,name varchar(10)) AUTO_ID_CACHE 1"

	db, err := sql.Open("mysql", flags.dsn)
	if err != nil {
		fmt.Println("conn to db server fail ,", err)
		return err
	}
	defer db.Close()
	_, err = db.Exec(createDBSQL)
	if err != nil {
		fmt.Println("create database fail,", err)
		return err
	}
	_, err = db.Exec(createTableSQL)
	if err != nil {
		fmt.Println("create table fail,", err)
		return err
	}
	return nil
}

func mustNil(err error) {
	if err != nil {
		panic(err)
	}
}

func handleQuery(wg *sync.WaitGroup) error {
	var num int64
	defer wg.Done()
	querysql := "insert into autoinc.autoinctest (name) values ('test')"
	db, err := sql.Open("mysql", flags.dsn)
	if err != nil {
		fmt.Println("conn to db server fail ,", err)
		return err
	}
	defer db.Close()
	tc := time.NewTicker(time.Duration(flags.runtime * 1000 * 1000 * 1000))
	for {
		select {
		case <-tc.C:
			MU.Lock()
			QUERY_COUNT += num
			MU.Unlock()
			return nil
		default:
			_, err := db.Exec(querysql)
			if err != nil {
				fmt.Println("exec insert fail,", err)
				return err
			}
			num++
		}
	}
}

func handlePrepare(wg *sync.WaitGroup) error {
	var num int64
	defer wg.Done()
	preparesql := "insert into autoinc.autoinctest (name) values (?)"
	val := "test"
	db, err := sql.Open("mysql", flags.dsn)
	if err != nil {
		fmt.Println("conn to db server fail ,", err)
		return err
	}
	defer db.Close()
	stmt, err := db.Prepare(preparesql)
	if err != nil {
		fmt.Println("prepare sql fail,", err)
		return err
	}
	tc := time.NewTicker(time.Duration(flags.runtime * 1000 * 1000 * 1000))
	for {
		select {
		case <-tc.C:
			MU.Lock()
			QUERY_COUNT += num
			MU.Unlock()
			return nil
		default:
			_, err := stmt.Exec(val)
			if err != nil {
				fmt.Println("exec insert fail,", err)
				return err
			}
			num++
		}
	}
}

func main() {
	flag.Parse()
	var rt int
	switch strings.ToLower(flags.runtype) {
	case "init":
		rt = 0
		err := createTable()
		mustNil(err)
		return
	case "run":
		rt = 1
	case "prepare":
		rt = 2
	default:
		err := errors.New("unsupported run type")
		mustNil(err)
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < flags.threads; i++ {
		wg.Add(1)
		if rt == 1 {
			err := handleQuery(&wg)
			mustNil(err)
		} else {
			err := handlePrepare(&wg)
			mustNil(err)
		}
	}
	wg.Wait()
	fmt.Println("QPS :", QUERY_COUNT/int64(flags.runtime))
}
