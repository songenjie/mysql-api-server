package api

import (
	"encoding/json"
	"io"
	"mysql-api-server/base"
	"mysql-api-server/dal"
	"log"
	"net/http"
)

func GetClusterList(w http.ResponseWriter, r *http.Request) {
	//set default result
	re := base.GetListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}

	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("GetClusterList ", re)
		res, _ := json.Marshal(re)
		io.WriteString(w, string(res))
	}()

	if result := dal.GetUsernameBysso(r); result.Message != "Success" {
		re.Message = result.Message
		re.Code = result.Code
		return
	}

	dal.GetClusterList(&re, *dal.Username)
}
