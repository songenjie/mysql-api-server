package main

import (
	"io"
	"mysql-api-server/api"
	"mysql-api-server/dal"
	"log"
	"net/http"
	"time"
)

func main() {
	//Updatea Superset db
	ticker := time.NewTicker(*dal.PersistenceDbsTTL)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case _ = <-ticker.C:
				if err := dal.Reload(); err != nil {
					log.Println(err)
				}
			}
		}
	}()
	now := time.Now()
	// 计算下一个零点
	next := now.Add(time.Hour * 24)
	// 定点
	next = time.Date(next.Year(), next.Month(), next.Day(), *dal.PartitionDbsTime, 0, 0, 0, next.Location())
	partitionTimer := time.NewTimer(next.Sub(now))
	defer partitionTimer.Stop()
	go func() {
		for {
			select {
			case _ = <-partitionTimer.C:
				if err := dal.Partition(); err != nil {
					log.Println(err)
				}
				dal.TruncateHiveLoadJob()
				now = next.Add(time.Hour * 24)
				partitionTimer = time.NewTimer(now.Sub(next))
			}
		}
	}()

	// index
	http.HandleFunc("/", HandleIndex)
	http.HandleFunc("/reload", Reload)
	// data import task
	http.HandleFunc("/SubmitImportTask", api.PostOnly(api.BasicAuth(api.SubmitImportTask)))
	http.HandleFunc("/GetImportTaskList", api.GetOnly(api.BasicAuth(api.GetImportTaskList)))
	http.HandleFunc("/GetClusterList", api.GetOnly(api.BasicAuth(api.GetClusterList)))
	http.HandleFunc("/GetDatabaseList", api.GetOnly(api.BasicAuth(api.GetDatabaseList)))
	http.HandleFunc("/GetTableList", api.GetOnly(api.BasicAuth(api.GetTableList)))
	http.HandleFunc("/GetPartitionList", api.GetOnly(api.BasicAuth(api.GetPartitionList)))
	http.HandleFunc("/GetMarketList", api.GetOnly(api.BasicAuth(api.GetMarketList)))
	http.HandleFunc("/GetProductionList", api.GetOnly(api.BasicAuth(api.GetProductionList)))
	http.HandleFunc("/GetQueueList", api.GetOnly(api.BasicAuth(api.GetQueueList)))
	http.HandleFunc("/ModificationStatus", api.PostOnly(api.BasicAuth(api.ModificationStatus)))
	http.HandleFunc("/ExportTask", api.GetOnly(api.BasicAuth(api.ExportTask)))

	log.Fatal(http.ListenAndServe("0.0.0.0:"+*dal.ListPort, nil))
}

func HandleIndex(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "msyql api: mysql name is songenjie welcome\n")
}

func Reload(w http.ResponseWriter, r *http.Request) {
	if err := dal.Reload(); err != nil {
		log.Println(err)
		io.WriteString(w, "superset Reload Error")
	}
	if err := dal.Partition(); err != nil {
		log.Println(err)
		io.WriteString(w, "partition Reload Error")
	}

	io.WriteString(w, "Success")
}
