package api

import (
	"encoding/json"
	"io"
	"mysql-api-server/base"
	"mysql-api-server/dal"
	"log"
	"net/http"
)

func GetPartitionList(w http.ResponseWriter, r *http.Request) {
	//set default result
	re := base.GetListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}

	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("GetPartitionList ", re)
		res, _ := json.Marshal(re)
		io.WriteString(w, string(res))
	}()
	//TODO: authentication
	//dal.init(r.Header.Get("username"),r.Header.Get("password"),userkey)
	//TODO: check params
	v := r.URL.Query()
	if v.Get("Cluster") == "" {
		re.Message = "Cluster Name  Cannot is null!"
		return
	}
	if v.Get("Database") == "" {
		re.Message = "Database Name  Cannot is null!"
		return
	}
	if v.Get("Table") == "" {
		re.Message = "Table Name  Cannot is null!"
		return
	}
	dal.GetPartitionList(&re, v.Get("Cluster"), v.Get("Database"), v.Get("Table"))
}
