package api

import (
	"encoding/json"
	"io"
	"mysql-api-server/base"
	"mysql-api-server/dal"
	"log"
	"net/http"
)

func GetDatabaseList(w http.ResponseWriter, r *http.Request) {
	//set default result
	re := base.GetListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}

	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("GetDatabaseList ", re)
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

	if result := dal.GetUsernameBysso(r); result.Message != "Success" {
		re.Message = result.Message
		re.Code = result.Code
		return
	}

	dal.GetDatabaseList(&re, *dal.Username, v.Get("Cluster"))
}
