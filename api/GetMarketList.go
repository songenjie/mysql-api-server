package api

import (
	"encoding/json"
	"io"
	"mysql-api-server/base"
	"mysql-api-server/dal"
	"log"
	"net/http"
)

//传入的参数为 martName and martProduction
func GetMarketList(w http.ResponseWriter, r *http.Request) {
	//set default result
	re := base.GetListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("GetMarketList ", re)
		res, _ := json.Marshal(re)
		io.WriteString(w, string(res))
	}()

	//TODO: authentication
	//dal.init(r.Header.Get("username"),r.Header.Get("password"),userkey)
	//TODO: check params

	if result := dal.GetUsernameBysso(r); result.Message != "Success" {
		re.Message = result.Message
		re.Code = result.Code
		return
	}
	dal.GetMarketList(&re, *dal.Username, "", true)
}
